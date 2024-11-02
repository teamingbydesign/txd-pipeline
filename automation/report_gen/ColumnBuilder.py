from typing import (
    Any,
    Callable,
    List,
    Dict,
    Type,
)

from utils import *


class ColumnBuilder:
    def __init__(
        self,
        col_prefix: str,
        col_suffix: str,
        pd_function: Callable[..., pd.DataFrame | pd.Series | int | float],
        pd_function_args: List[Any]
    ):

        self.col_prefix = col_prefix
        self.col_suffix = col_suffix
        self.pd_function = pd_function
        self.pd_function_args = pd_function_args

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        new_col_name = f'{self.col_prefix}{self.col_suffix}'
        pd_result = self.pd_function(df, *self.pd_function_args)
        # if unequal length of series, need to perform merge on index of result
        if isinstance(pd_result, pd.DataFrame) and len(pd_result) != len(df):
            index_name = pd_result.index.name
            pd_result.columns = pd.Index([new_col_name])

            return pd.merge(df, pd_result, how='left', on=index_name)

        # singular value will be broadcast on whole column
        # or same length of series and dataframe
        else:
            return df.assign(**{
                new_col_name: self.pd_function(df, *self.pd_function_args)
            })


class Identity(ColumnBuilder):
    def __init__(self, col_prefix: str, col_suffix: str, question: str):
        super().__init__(col_prefix, col_suffix, lambda df, q: df[q], [question])


class ClassAvg(ColumnBuilder):
    def __init__(self, col_prefix: str, question: str):
        super().__init__(col_prefix, 'ClassAvg', get_class_average, [question])


class ClassStDev(ColumnBuilder):
    def __init__(self, col_prefix: str, question: str):
        super().__init__(col_prefix, 'ClassStDev', get_class_stdev, [question])


class TeammateAvg(ColumnBuilder):
    def __init__(self, col_prefix: str, question: str):
        super().__init__(col_prefix, 'TeammateAvg', get_teammates_average, [question])

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        result = super().__call__(df)
        return result


class TeamAvg(ColumnBuilder):
    def __init__(self, col_prefix: str, question: str):
        super().__init__(col_prefix, 'TeamAvg', get_team_average, [question])


class AcrossTeammates(ColumnBuilder):
    # only use this for class avg across all teammate responses
    pass


class PerMemberFunc(ColumnBuilder):
    # no need to override __init__

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        team_size = max(df['TeammateNumber'].unique())

        for i in range(1, team_size+1):
            df = ColumnBuilder(
                self.col_prefix,
                str(i),
                get_response_by_teammate_number,
                [i] + self.pd_function_args
            )(df)

        return df


SUFFIX_TO_COLUMNBUILDER: Dict[str, Type[ColumnBuilder]] = {
    'TeamAvg': TeamAvg,
    'TeammateAvg': TeammateAvg,
    '': Identity,
    'Me': Identity,
    'ClassAvg': ClassAvg,
    'ClassStDev': ClassStDev,
    'PerMember': PerMemberFunc,
}
