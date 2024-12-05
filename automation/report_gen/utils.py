import numpy as np
import pandas as pd

import warnings
from typing import Optional

def _keep_numeric_cols(df: pd.DataFrame, question: str):
    relevant_columns = df.loc[:, df.columns.str.startswith(question)]
    # get only numeric columns
    keep_cols = []
    for idx, row in pd.DataFrame(relevant_columns.dtypes).iterrows():
        if row[0] in ['int', 'float']:
            keep_cols.append(idx)

    relevant_columns = df[keep_cols]

    return relevant_columns


def get_team_average(df: pd.DataFrame, question: str):
    # Gets average on specified question for each team in df
    return df[['TeamName', question]].groupby('TeamName').mean()


def get_teammates_average(df: pd.DataFrame, question: str):
    def apply_helper(row, apply_df):
        team_number, teammate_number = row['TeamName'], int(row['TeammateNumber'])
        apply_df = apply_df.loc[
            (apply_df['TeamName'] == team_number) & (apply_df['TeammateNumber'] != teammate_number),
            f'{question}.{teammate_number}'
        ]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            return np.nanmean(apply_df)

    return df.apply(apply_helper, axis=1, apply_df=df)


def get_teammates_std(df: pd.DataFrame, question: str):
    def apply_helper(row, apply_df):
        team_number, teammate_number = row['TeamName'], int(row['TeammateNumber'])
        apply_df = apply_df.loc[
            (apply_df['TeamName'] == team_number) & (apply_df['TeammateNumber'] != teammate_number),
            f'{question}.{teammate_number}'
        ]
        return np.nanstd(apply_df)

    return df.apply(apply_helper, axis=1, df=df)


def get_response_by_teammate_number(df: pd.DataFrame, teammate_number: int, question: str):
    if f'{question}.{teammate_number}' not in df.columns:
        return df.loc[df['TeammateNumber'] == teammate_number, question]

    team_sizes = df[['TeamName']].groupby('TeamName').size()
    names = df['FullNameLast']
    df = df.loc[df['TeammateNumber'] == teammate_number, df.columns.str.contains(f'^{question}|Team', regex=True)]

    #this is for debugging help when there is issue w/ data
    seen_indexes = set()
    result = [[], []]
    for index, row in df.iterrows():
        team_size = team_sizes[row['TeamName']]

        for i in range(1, team_size+1):
            if f'{question}.{i}' not in df.columns:
                break
            if index + i - teammate_number in seen_indexes:
                print(index, i, teammate_number)
            seen_indexes.add(index + i - teammate_number)
            result[0].append(index + i - teammate_number)
            result[1].append(row[f'{question}.{i}'])

    result_series = pd.Series(result[1])
    result_series.index = result[0]
    result_df = result_series.to_frame()

    result_df = result_df.join(names).set_index('FullNameLast')

    return result_df


def get_my_response(df: pd.DataFrame, question: str) -> pd.Series:
    def apply_helper(row):
        teammate_number = int(row['TeammateNumber'])
        return row[f'{question}.{teammate_number}']

    return df.apply(apply_helper, axis=1)


def get_class_average(df: pd.DataFrame, question: str) -> float:
    # Returns the class average for QUESTION
    return float(np.mean(df[question]))


def get_class_average_all_teammates(df: pd.DataFrame, question: str) -> float:
    return np.nanmean(_keep_numeric_cols(df, question))


def get_class_stdev(df: pd.DataFrame, question: str) -> float:
    # Returns the class std dev for QUESTION
    return float(np.std(df[question]))


def get_class_stdev_all_teammates(df: pd.DataFrame, question: str) -> float:
    return np.nanstd(_keep_numeric_cols(df, question))

def get_alignment_level(df: pd.DataFrame, align_cols: list[str], bin_names: Optional[list[str]] = None) -> pd.Series:
    # Adhoc calculation for getting alignment level of team based on ntile
    # Not used in ColumnBuilder

    if not bin_names:
        bin_names = ["weak", "standard", "strong", "exceptional"]

    align_cols = [col + 'TeamAvg' for col in align_cols if col + 'TeamAvg' in df.columns]
    df_by_team = df[['TeamName'] + align_cols].groupby('TeamName').first()

    totals = df_by_team[align_cols].sum(axis=1)

    percentiles = [100 // len(bin_names) * (i+1) for i in range(len(bin_names))]
    bins = np.percentile(totals, percentiles)

    indexes = np.searchsorted(bins, totals, side='left')

    result = pd.Series(np.vectorize(lambda x: bin_names[x])(indexes))

    result_df = pd.DataFrame.from_dict({'TeamName': df_by_team.index.values, 'OverallAlignment': result})

    return df.merge(result_df, how='inner', left_on='TeamName', right_on='TeamName')
