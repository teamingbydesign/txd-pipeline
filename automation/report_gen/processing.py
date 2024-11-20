import pandas as pd
import numpy as np
import re

# TODO: Alex to change this function into a class;
#  utilize OOP methods and split this into helper functions to improve future debugging
# TODO: Alex to add unit tests for this process and validation processes (other than current 1 exception case)


# suggestion: move these mappings to another CSV to join with question_dictionary
SEVEN_SCALE_MAPPINGS = {
    "STRONGLY AGREE": 7,
    "AGREE": 6,
    "SOMEWHAT AGREE": 5,
    "NEITHER AGREE NOR DISAGREE": 4,
    "SOMEWHAT DISAGREE": 3,
    "DISAGREE": 2,
    "STRONGLY DISAGREE": 1,
    np.nan: np.nan,
    "MUCH BETTER": 7,
    "MODERATELY BETTER": 6,
    "SLIGHTLY BETTER": 5,
    "ABOUT THE SAME": 4,
    "SLIGHTLY WORSE": 3,
    "MODERATELY WORSE": 2,
    "MUCH WORSE": 1,
}
FIVE_SCALE_MAPPINGS = {
    "ALWAYS": 5,
    "VERY OFTEN": 4,
    "SOMETIMES": 3,
    "RARELY": 2,
    "NEVER": 1,
    np.nan: np.nan
}


def _handle_quantitative(
        full_cleaned: pd.DataFrame,
        question_dictionary: pd.DataFrame,
        needs_mapping: bool,
        needs_normalization: bool
) -> pd.DataFrame:
    """
    Helper function used by cleanQualtricsData to map qualitative responses into quantities
    to be used for metrics.

    :param full_cleaned: DataFrame of cleaned and formatted survey responses prior
           to handling quantitative
    :param question_dictionary: DataFrame of question metadata (used for checking any quantitative questions)
    :param needs_normalization: Flag determining if quantitative responses should be centered on 0.
    :return: DataFrame after quantitative mapping is done
    """
    quantitative_questions = list(
        question_dictionary[question_dictionary["type"] == "quantitative"]["question_id"]
    )

    # convert qualitative response to quantity based on 5-scale or 7-scale mapping
    for question in quantitative_questions:
        denominator = int(question_dictionary[question_dictionary["question_id"] == question]["out_of"].iloc[0])
        question_str = str(question)
        if needs_mapping:
            if denominator == 7:
                full_cleaned[question_str] = full_cleaned[question_str].str.upper().apply(lambda x: SEVEN_SCALE_MAPPINGS[x])
                if needs_normalization:
                    full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str]) - 4

            elif denominator == 5:
                full_cleaned[question_str] = full_cleaned[question_str].str.upper().apply(lambda x: FIVE_SCALE_MAPPINGS[x])
                if needs_normalization:
                    full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str]) - 3

        # if question is quantitative but dtype is a str, change data type
        else:
            full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str])

    return full_cleaned


def clean_qualtrics_data(
        raw_df: pd.DataFrame,
        roster_df: pd.DataFrame,
        question_dictionary_df: pd.DataFrame,
        needs_mapping: bool = True,
        needs_normalization: bool = True
) -> pd.DataFrame:
    """
    This function handles all data cleaning steps from the raw survey responses,
    roster, and question details to combine into one main csv for
    calculating metrics for the faculty report.


    :param raw_df: dataframe of raw survey responses
    :param roster_df: dataframe of roster of students in class as csv
    :param question_dictionary_df: dataframe question descriptions (qualitative/quantitative, etc.) as csv
    :param needs_mapping: Flag that handles mapping of qualitative responses to quantitative columns
    :param needs_normalization: Flag that normalizes the quantitative values after mapping,
           can only be used if needs_mapping is True
    :return: DataFrame after all cleaning is done
    """

    # trim duplicated column headers
    if raw_df.iloc[0, 1] == raw_df.columns[0]:
        raw_df = raw_df.iloc[1:, :]
    # trim importid like row
    if 'ImportId' in raw_df.iloc[0, 1]:
        raw_df = raw_df.iloc[1:, 1]

    raw_df = raw_df.rename(columns={'Unnamed: 0': 'FullNameLast'})

    # Subset raw data to just student email, student first name, student last name, and all question responses
    subset_data = raw_df.filter(regex=r'Email|First|Last|Name|Q\d+(_\d+)?', axis=1)

    # Replace question column names in subset data with X.Y instead of QX_Y
    subset_data.columns = [col.replace('Q', '').replace('_', '.') for col in list(subset_data.columns)]
    subset_data.columns = [col + ".0" if re.match(r'^[0-9]', col) and "." not in col else col for col in list(subset_data.columns)]

    # Instantiate cleaned, a pointer of subset_data
    cleaned = subset_data.copy()

    # Join in TeamNumber and TeammateNumber from roster; drop rows of metadata
    roster_email_field = [col for col in list(roster_df.columns) if 'EMAIL' in col.upper()][0]
    cleaned_email_field = [col for col in list(cleaned.columns) if 'EMAIL' in col.upper()][0]
    roster_df[roster_email_field] = roster_df[roster_email_field].str.lower()
    cleaned[cleaned_email_field] = cleaned[cleaned_email_field].str.lower()

    full_cleaned = pd.merge(roster_df[["FullNameLast", 'TeamName', 'TeammateNumber']],
                            cleaned,
                            how="outer",
                            left_on="FullNameLast",
                            right_on="FullNameLast")

    # full_cleaned = pd.merge(roster_df[[roster_email_field, 'GroupNumber', 'TeammateNumber']],
    #                         cleaned,
    #                         how="outer",
    #                         left_on=roster_email_field,
    #                         right_on=cleaned_email_field)

    full_cleaned = full_cleaned[~full_cleaned["TeamName"].isna()]

    # Sort df by TeamNumber then TeammateNumber starting from Team1
    full_cleaned = full_cleaned.sort_values(["TeamName", "TeammateNumber"]).reset_index().drop("index", axis=1)

    # If the raw data has "Agree"/"Disagree" in quantitative question columns,
    # map these to integers 1-X where X is "out_of"
    full_cleaned = _handle_quantitative(full_cleaned, question_dictionary_df, needs_mapping, needs_normalization)

    # For NA values (students that didn't complete survey, left question empty), fill in with "No Response"
    for col in full_cleaned:
        dt = full_cleaned[col].dtype
        if dt == 'object':
            full_cleaned[col] = full_cleaned[col].fillna('No Response')

    # full_cleaned['TeamNumber'] = full_cleaned['TeamNumber'].astype('int')
    full_cleaned['TeammateNumber'] = full_cleaned['TeammateNumber'].astype('int')

    return full_cleaned
