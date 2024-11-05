import pandas as pd
import numpy as np
import configparser
from typing import (
    Tuple,
    Optional,
    Callable
)
from gdrive_import import retrieve_teaming_files
from processing import clean_qualtrics_data
from ColumnBuilder import *
from report_definition import pipe_params
from upload_mongo import upload_df_to_mongodb

# read in configuration
cfg = configparser.ConfigParser()
cfg.read('config.ini')


def prefix_translator(column_to_prefix: Dict[str, str], question: str) -> Optional[str]:
    """
    Sometimes in the question dictionary mapping, rows contain the same question
    duplicated for each team member, like "5.1" -> "Work Allocation Points".
    For the pipeline, sometimes the params need column "5" and not "5.1", so
    so this function helps resolve this edge case.

    :param column_to_prefix: mapping of column id to prefix
    :param question: column_id (possibly truncated) to convert to prefix
    :return: prefix if the question exists in the mapping, otherwise None
    """

    if question in column_to_prefix:
        return column_to_prefix[question]

    if f'{question}.1' in column_to_prefix:
        return column_to_prefix[f'{question}.1']

    return None


def column_builder_pipe(
        df: pd.DataFrame,
        column_builder_params: List[Tuple[str, str, Optional[Callable], Optional[List[Any]]]],
        column_to_prefix: Dict[str, str]
) -> pd.DataFrame:
    """
    This is the main handler of building the new columns based on defined
    operations in column_builder_params. The function reads in the parameters,
    creates an appropriate ColumnBuilder object and invokes it on the
    dataframe.

    :param df: DataFrame to perform column building operations on
    :param column_builder_params: List of tuples that have the question id,
           suffix that explains the operation being done, an optional
           custom function, and optional parameters passed to the function
    :param column_to_prefix: mapping of column id to prefix
    :return: resulting DataFrame after all operations are performed
    """
    for (question, suffix, func, func_args) in column_builder_params:
        prefix = prefix_translator(column_to_prefix, question)
        if prefix is None:
            print(f"Question {question} does not exist.")
            continue
        if suffix in SUFFIX_TO_COLUMNBUILDER:
            builder_type = SUFFIX_TO_COLUMNBUILDER[suffix]

            if builder_type == PerMemberFunc:
                cb = builder_type(prefix, suffix, func, func_args)
            elif func is not None:
                cb = ColumnBuilder(prefix, suffix, func, func_args)
            else:
                if builder_type == Identity:
                    cb = builder_type(prefix, suffix, question)
                elif builder_type == PerMemberFunc:
                    cb = builder_type(prefix, suffix, func, func_args)
                else:
                    cb = builder_type(prefix, question)

        else:
            cb = ColumnBuilder(prefix, suffix, func, func_args)

        df = df.pipe(cb)

    return df


def main(
    class_name: str,
    checkin_num: str,
):
    """
    Main function that handles all reading, operations, and writing of
    csv files. Basically invoke this to run the pipeline completely through.

    :param class_name:
    :param checkin_num:
    :return:
    """
    dataframes = retrieve_teaming_files(class_name, checkin_num)
    raw = dataframes['raw']
    roster = dataframes['roster']
    dictionary = dataframes['question_dictionary']

    # this is temp fix for differing column names
    roster = roster.rename(columns={'GroupNumber': 'TeamNumber'})

    if raw is None or roster is None or dictionary is None:
        raise Exception("Could not find all dataframes")

    full_cleaned = clean_qualtrics_data(raw, roster, dictionary, needs_mapping=False)

    # convert TeamNumber to float
    for df in (roster, full_cleaned):
        if df['TeamNumber'].dtype == 'object':
            df['TeamNumber'] = df['TeamNumber'].str.replace('Team ', '', regex=False).astype(float)
        else:
            df['TeamNumber'] = df['TeamNumber'].astype(float)

    # convert TeammateNumber to float
    roster['TeammateNumber'] = roster['TeammateNumber'].astype(float)

    # column to prefix is stored in dictionary `shorthand` col
    dictionary.set_index('question_id', inplace=True)
    dictionary.index = dictionary.index.map(str)
    column_to_prefix = dict(dictionary['shorthand'])

    result = column_builder_pipe(full_cleaned, pipe_params, column_to_prefix)

    # cleanup columns and do post-processing
    cols_to_remove = ~full_cleaned.columns.str.contains(r'[0-9]', regex=True)
    padded = list(cols_to_remove) + [np.True_] * (len(result.columns) - len(cols_to_remove))
    result = result.loc[:, np.array(padded)]
    result = result.fillna(value='No Response')
    result.to_csv('report.csv')

    # upload to mongodb
    mongo_uri = f"mongodb+srv://{cfg['mongodb']['username']}:{cfg['mongodb']['password']}@{cfg['mongodb']['host']}"

    upload_df_to_mongodb(
        df=result,
        db_name=cfg['mongodb']['db'],
        collection_name=f'{class_name}_CHECKIN{checkin_num}_REPORT',
        mongo_uri=mongo_uri,
    )


if __name__ == '__main__':
    main(
        "OCONNELL",
        "01"
    )
