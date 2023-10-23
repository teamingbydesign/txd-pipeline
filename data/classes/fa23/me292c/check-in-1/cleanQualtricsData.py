import pandas as pd
import numpy as np

# TODO: Alex to change this function into a class; utilize OOP methods and split this into helper functions to improve future debugging
# TODO: Alex to add unit tests for this process and validation processes (other than current 1 exception case)

def cleanQualtricsData(raw, roster, question_dictionary, needsMapping=True, needsNormalization=True):

    #assert all files are of .csv extension 
    if not any(["CSV" in [raw.upper(), roster.upper(), question_dictionary.upper()]]):
        raise Exception("At least one of the input files is not in CSV format")
    

    # Imports and Instantiations
    raw = pd.read_csv(raw)
    question_dictionary = pd.read_csv(question_dictionary)
    roster =  pd.read_csv(roster)
    quantitative_questions = list(question_dictionary[question_dictionary["type"] == "quantitative"]["question_id"])
    
    # Subset raw data to just student email, student first name, student last name, and all question responses 
    cols_needed = []
    keywords_list = ['EMAIL', 'FIRST', 'LAST', 'Q']
    for col in list(raw.columns):
        if any(keyword in col.upper() for keyword in keywords_list):
            cols_needed.append(col)
    subset_data = raw[cols_needed]

    # Replace question column names in subset data with X.Y instead of QX_Y
    subset_data.columns = [col.replace('Q', '').replace('_', '.') for col in list(subset_data.columns)]

    # Instantiate cleaned, a pointer of subset_data
    cleaned = subset_data

    # Join in TeamNumber and TeammateNumber from roster; drop rows of metadata
    roster_email_field = [col for col in list(roster.columns) if 'EMAIL' in col.upper()][0]
    cleaned_email_field = [col for col in list(cleaned.columns) if 'EMAIL' in col.upper()][0]

    full_cleaned = pd.merge(roster[[roster_email_field, 'TeamNumber', 'TeammateNumber']], 
                            cleaned, 
                            how="outer", 
                            left_on = roster_email_field, 
                            right_on = cleaned_email_field)
    full_cleaned = full_cleaned[~full_cleaned["TeamNumber"].isna()]

    # Sort df by TeamNumber then TeammateNumber starting from Team1
    full_cleaned = full_cleaned.sort_values(["TeamNumber", "TeammateNumber"]).reset_index().drop("index", axis=1)
    
    # If the raw data has "Agree"/"Disagree" in quantitative question columns, map these to integers 1-X where X is "out_of"
    if needsMapping:
        seven_scale_mappings = {
            "STRONGLY AGREE" : 7,
            "AGREE" : 6,
            "SOMEWHAT AGREE" : 5,
            "NEITHER AGREE NOR DISAGREE" : 4,
            "SOMEWHAT DISAGREE" : 3,
            "DISAGREE" : 2,
            "STRONGLY DISAGREE" : 1,
            np.nan : np.nan,
            "MUCH BETTER" : 7,
            "MODERATELY BETTER" : 6,
            "SLIGHTLY BETTER" : 5,
            "ABOUT THE SAME" : 4,
            "SLIGHTLY WORSE" : 3,
            "MODERATELY WORSE" : 2,
            "MUCH WORSE" : 1,
        }
        five_scale_mappings = {
            "ALWAYS" : 5,
            "VERY OFTEN" : 4,
            "SOMETIMES" : 3,
            "RARELY" : 2,
            "NEVER" : 1,
            np.nan : np.nan
        }

        for question in quantitative_questions:
            denominator = int(question_dictionary[question_dictionary["question_id"] == question]["out_of"])
            question_str = str(question)
            
            if denominator == 7:
                full_cleaned[question_str] = full_cleaned[question_str].str.upper()
                full_cleaned[question_str] = [seven_scale_mappings[response] for response in full_cleaned[question_str]]
            if denominator == 5:
                full_cleaned[question_str] = full_cleaned[question_str].str.upper()
                full_cleaned[question_str] = [five_scale_mappings[response] for response in full_cleaned[question_str]]
                
            # if question is quantitative but dtype is a str, change data type
            if full_cleaned[[question_str]].dtypes[0] == str:
                full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str])
        
    # If normalization is true, then all quantitative data is normed to 0. Assumed question_dictionary includes "out_of" column
    if needsNormalization:
        for question in quantitative_questions:
            denominator = int(question_dictionary[question_dictionary["question_id"] == question]["out_of"])
            question_str = str(question)
            
            if denominator == 7:
                full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str]) - 4
            if denominator == 5:
                full_cleaned[question_str] = pd.to_numeric(full_cleaned[question_str]) - 3
                
    # For NA values (students that didn't complete survey, left question empty), fill in with "No Response"
    full_cleaned = full_cleaned.fillna('No Response')
    
    return full_cleaned
