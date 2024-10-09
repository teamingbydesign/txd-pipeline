import os

import pandas as pd
import numpy as np

# TODO: Alex to change this function into a class; utilize OOP methods and split this into helper functions to improve future debugging
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
        needs_normalization: bool
):
    quantitative_questions = list(
        question_dictionary[question_dictionary["type"] == "quantitative"]["question_id"]
    )

    for question in quantitative_questions:
        denominator = int(question_dictionary[question_dictionary["question_id"] == question]["out_of"].iloc[0])
        question_str = str(question)

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

def cleanQualtricsData(
        raw: str | os.PathLike,
        roster: str | os.PathLike,
        question_dictionary: str | os.PathLike,
        needs_mapping: bool = True,
        needs_normalization: bool = True
):

    # assert all files are of .csv extension
    assert all(
        [os.path.splitext(filepath)[1] == '.csv'
         for filepath in (raw, roster, question_dictionary)]
    )

    # Imports and Instantiations
    # TODO: this has to get fixed, first row is repeat of headers, 2nd is random import stuff
    raw_df = pd.read_csv(raw)[2:]
    question_dictionary_df = pd.read_csv(question_dictionary)
    roster_df = pd.read_csv(roster)

    # Subset raw data to just student email, student first name, student last name, and all question responses
    subset_data = raw_df.filter(regex=r'Email|First|last|Q\d+(_\d+)?', axis=1)

    # Replace question column names in subset data with X.Y instead of QX_Y
    subset_data.columns = [col.replace('Q', '').replace('_', '.') for col in list(subset_data.columns)]

    # Instantiate cleaned, a pointer of subset_data
    cleaned = subset_data

    # Join in TeamNumber and TeammateNumber from roster; drop rows of metadata
    roster_email_field = [col for col in list(roster_df.columns) if 'EMAIL' in col.upper()][0]
    cleaned_email_field = [col for col in list(cleaned.columns) if 'EMAIL' in col.upper()][0]

    full_cleaned = pd.merge(roster_df[[roster_email_field, 'TeamNumber', 'TeammateNumber']],
                            cleaned,
                            how="outer",
                            left_on=roster_email_field,
                            right_on=cleaned_email_field)
    full_cleaned = full_cleaned[~full_cleaned["TeamNumber"].isna()]

    # Sort df by TeamNumber then TeammateNumber starting from Team1
    full_cleaned = full_cleaned.sort_values(["TeamNumber", "TeammateNumber"]).reset_index().drop("index", axis=1)

    # If the raw data has "Agree"/"Disagree" in quantitative question columns, map these to integers 1-X where X is "out_of"
    if needs_mapping:
        full_cleaned = _handle_quantitative(full_cleaned, question_dictionary_df, needs_normalization)

    # For NA values (students that didn't complete survey, left question empty), fill in with "No Response"
    # TODO: impute by dtype of column rather than general "No Response"
    for col in full_cleaned:
        dt = full_cleaned[col].dtype
        if dt == 'object':
            full_cleaned[col].fillna('No Response', inplace=True)

    return full_cleaned
