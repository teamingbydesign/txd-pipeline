import numpy as np
import pandas as pd


def _keep_numeric_cols(df, question):
    relevant_columns = df.loc[:, df.columns.str.startswith(question)]
    # get only numeric columns
    keep_cols = []
    for idx, row in pd.DataFrame(relevant_columns.dtypes).iterrows():
        if row[0] in ['int', 'float']:
            keep_cols.append(idx)

    relevant_columns = df[keep_cols]

    return relevant_columns


def get_team_average(df, question):
    # Gets average on specified question for each team in df
    return df[['TeamNumber', question]].groupby('TeamNumber').mean()


def get_teammates_average(df, question):
    def apply_helper(row, apply_df):
        team_number, teammate_number = row['TeamNumber'], int(row['TeammateNumber'])
        apply_df = apply_df.loc[
            (apply_df['TeamNumber'] == team_number) & (apply_df['TeammateNumber'] != teammate_number),
            f'{question}.{teammate_number}'
        ]
        return np.nanmean(apply_df)

    return df.apply(apply_helper, axis=1, apply_df=df)


def get_teammates_std(df: pd.DataFrame, question: str):
    def apply_helper(row, apply_df):
        team_number, teammate_number = row['TeamNumber'], int(row['TeammateNumber'])
        apply_df = apply_df.loc[
            (apply_df['TeamNumber'] == team_number) & (apply_df['TeammateNumber'] != teammate_number),
            f'{question}.{teammate_number}'
        ]
        return np.nanstd(apply_df)

    return df.apply(apply_helper, axis=1, df=df)


def get_response_by_teammate_number(df, teammate_number, question):
    return df.loc[:, f'{question}.{teammate_number}']


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
