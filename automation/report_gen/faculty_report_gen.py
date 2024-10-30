import os
import argparse
import configparser
from typing import (
    Tuple,
    Optional
)
from processing import clean_qualtrics_data
from ColumnBuilder import *
from report_definiton import pipe_params
from upload_mongo import upload_df_to_mongodb


cfg = configparser.ConfigParser()
cfg.read('report_gen\\config.ini')

def prefix_translator(column_to_prefix: Dict[str, str], question: str) -> Optional[str]:
    if question in column_to_prefix:
        return column_to_prefix[question]

    if f'{question}.1' in column_to_prefix:
        return column_to_prefix[f'{question}.1']

    return None


def column_builder_pipe(
        df: pd.DataFrame,
        column_builder_params: List[Tuple],
        column_to_prefix: Dict[str, str]
) -> pd.DataFrame:
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
    roster_path: os.PathLike | str,
    question_dict_path: os.PathLike | str,
    raw_path: os.PathLike | str
):
    assert all(
        [os.path.splitext(filepath)[1] == '.csv'
         for filepath in (raw_path, roster_path, question_dict_path)]
    )

    roster = pd.read_csv(roster_path)
    dictionary = pd.read_csv(question_dict_path)
    raw = pd.read_csv(raw_path)

    full_cleaned = clean_qualtrics_data(raw, roster, dictionary)

    # convert TeamNumber to float
    for df in (roster, full_cleaned):
        df['TeamNumber'] = df['TeamNumber'].str.replace('Team ', '', regex=False).astype(float)

    # convert TeammateNumber to float
    roster['TeammateNumber'] = roster['TeammateNumber'].astype(float)

    # column to prefix is stored in dictionary `shorthand` col
    dictionary.set_index('question_id', inplace=True)
    dictionary.index = dictionary.index.map(str)
    column_to_prefix = dict(dictionary['shorthand'])
    print(column_to_prefix)

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
        collection_name=cfg['mongodb']['collection'],
        mongo_uri=mongo_uri,
    )


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument("roster_path")
    # parser.add_argument("question_dict_path")
    # parser.add_argument("raw_path")
    #
    # args = parser.parse_args()
    #
    # main(
    #     args.roster_path,
    #     args.question_dict_path,
    #     args.raw_path
    # )


    main(
        "E29_Qualtrics_Roster_EOS.csv",
        "E29_QUESTION_DICTIONARY.csv",
        "E29_PRECLEAN_CHECKIN03_RAW_TEXT.csv"
    )
