#notes

import pandas as pd
import re

def cleanQualtricsData(raw, roster, question_dictionary, normalization=True):

    #assert all files are of .csv extension 
    #assert all needed columns are available

    # Imports
    raw = pd.read_csv(raw)
    question_dictionary = pd.read_csv(question_dictionary)
    roster =  pd.read_csv(roster)
    
    # Subset raw data to just student email, student first name, student last name, and all question responses 
    cols_needed = []
    keywords_list = ['EMAIL', 'FIRST', 'LAST', 'Q']
    for col in list(raw.columns):
        if any(keyword in UPPER(col) for keyword in keywords_list):
            cols_needed.append(col)
    subset_data = raw[cols_needed]

    # Replace question column names in subset data with X.Y instead of QX_Y
    subset_data.columns = [col.replace('Q', '').replace('_', '.') for col in list(subset_data.columns)]

    # Instantiate cleaned, a pointer of subset_data
    cleaned = subset_data

    # Join in TeamNumber and TeammateNumber from roster; drop rows of metadata
    roster_email_field = [col for col in list(roster.columns) if 'EMAIL' in UPPER(col)][0]
    cleaned_email_field = [col for col in list(cleaned.columns) if 'EMAIL' in UPPER(col)][0]

    full_cleaned = pd.merge(roster[[roster_email_field, 'TeamNumber', 'TeammateNumber']], 
                            cleaned, 
                            how="outer", 
                            left_on = roster_email_field, 
                            right_on = cleaned_email_field)
    full_cleaned = full_cleaned[~full_cleaned[cleaned_email_field].isna()]
    
    # Cast TeamNumber and TeammateNumber to int
    full_cleaned["TeamNumber"] = [int(i) for i in list(full_cleaned["TeamNumber"])] 
    full_cleaned["TeammateNumber"] = [int(i) for i in list(full_cleaned["TeammateNumber"])] 

    # Sort df by TeamNumber then TeammateNumber starting from Team1
    full_cleaned = full_cleaned.sort_values(["TeamNumber", "TeammateNumber"]).reset_index().drop("index", axis=1)

    # For NA values (students that didn't complete survey, left question empty), fill in with "No Response"
    full_cleaned = full_cleaned.fillna('No Response')

    # If normalization is true, then all quantitative data is normed to 0. Assumed question_dictionary includes "out_of" column
    if normalization:
        questions_to_normalize_list = question_dictionary[question_dictionary["type" == "quantitative"]]["question_id"]
        for question in questions_to_normalize_list:
            if question_dictionary[question_dictionary["question_id"] == question]["out_of"] == 7:
                full_cleaned[question] = full_cleaned[question] - 4
            if question_dictionary[question_dictionary["question_id"] == question]["out_of"] == 5:
                full_cleaned[question] = full_cleaned[question] - 3

    return full_cleaned





