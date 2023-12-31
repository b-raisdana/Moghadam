from __future__ import annotations

import os
from typing import Type, Union, Callable

import pandas as pd
import pandera
from pandera import typing as pt

from Config import config
from data_preparation import empty_df, concat, read_with_timeframe, after_under_process_date, datarange_is_not_cachable


class BasePanderaDFM(pandera.DataFrameModel):
    class Config:
        # to resolve pandera.errors.SchemaError: column ['XXXX'] not in dataframe
        add_missing_columns = True
        # to resolve pandera.errors.SchemaError: expected series ['XXXX'/None] to have type datetime64[ns, UTC]
        # , got object
        coerce = True

    @classmethod
    def empty(cls):
        if cls._sample_obj is None:
            raise Exception("_sample_obj should be set manually before.")
        return cls._sample_obj.drop(cls._sample_obj.index)


class ExpandedDf:
    schema_data_frame_model: BasePanderaDFM = None
    _sample_df: pd.DataFrame = None
    _empty_df: pd.DataFrame = None

    @classmethod
    def _set_index(cls, data: pt.DataFrame[BasePanderaDFM]) -> BasePanderaDFM:
        if 'date' not in data.columns:
            raise Exception(f"Expected to have a 'date' column in data")
        if hasattr(data.index, 'names') and 'timeframe' in data.index.names:
            if 'timeframe' not in data.columns:
                raise Exception(f"Expected to have a 'timeframe' column in data")
            return data.set_index(['timeframe', 'date'])
        else:
            return data.set_index(['date'])

    @classmethod
    def new(cls, dictionary_of_data: dict = {}) -> 'BasePanderaDFM':
        if cls._sample_df is None:
            raise Exception("{}._sample_obj should be defined before!")
        if len(dictionary_of_data) > 0:
            if 'date' not in dictionary_of_data.keys():
                raise Exception("'date' is the mandatory TimestampIndex and is required!")
            _new = pt.DataFrame[cls.schema_data_frame_model](dictionary_of_data)
            _new = cls._set_index(_new)
            return _new
        if cls._empty_df is None:
            cls._empty_df = cls._sample_df.drop(cls._sample_df.index)
            cls._empty_df = cls._set_index(cls._empty_df)
        _new = cls._empty_df.copy()
        return _new

    @classmethod
    def cast_and_validate(cls: Type['ExpandedDf'], df: Union['ExpandedDf', pd.DataFrame],
                          inplace: bool = True, return_bool: bool = False, zero_size_allowed: bool = False,
                          log_D=None) -> Union['BasePanderaDFM', bool]:
        if not zero_size_allowed:
            if len(df) == 0:
                raise Exception('Zero size data not allowed in parameters!')
        try:
            result = cls.schema_data_frame_model.validate(df)
        except pandera.errors.SchemaError as e:
            if return_bool:
                log_D(f"data is not valid according to {cls.schema_data_frame_model.__name__}")
                return False
            else:
                raise e
        if return_bool:
            return True
        if inplace:
            df.__dict__ = result.__dict__
            return df
        else:
            return result

    # @classmethod
    # def zz_new(cls: Type['ExpandedDf'], **kwargs) -> 'ExpandedDf':
    #     # todo: test
    #     if ((not hasattr(cls, 'schema_data_frame_model')) and (cls.schema_data_frame_model is not None)):
    #         # or (not issubclass(cls.schema_data_frame_model, pt.Dataframe))):
    #         raise Exception(
    #             f"{cls.__name__}.schema_data_frame_model should be defined as a subclass of pandera.typing.Dataframe before calling {cls.__name__}.new(...)")
    #     result = empty_df(cls.schema_data_frame_model)
    #     if len(kwargs) > 0:
    #         d_types = dict(column_fields(cls.schema_data_frame_model),
    #                        **index_fields(cls.schema_data_frame_model))  # all_annotations(cls.schema_data_frame_model)
    #         # # check if all Series fields of required self.schema_data_frame_model are present in kwargs
    #         # required_fields = set(_all_annotations.keys())
    #         # provided_fields = set(kwargs.keys())
    #         # missing_fields = required_fields - provided_fields
    #         # if missing_fields:
    #         #     raise ValueError(f"Missing required fields in kwargs: {missing_fields}")
    #         # todo: test
    #         if not 'date' in kwargs.keys():
    #             raise Exception("'date' is the mandatory TimestampIndex and is required!")
    #         date = kwargs['date']
    #         # check if all of kwargs keys are in Series fields
    #         invalid_fields = [field for field in kwargs.keys() if field not in d_types.keys()]
    #         if len(invalid_fields) > 0:
    #             raise ValueError(
    #                 f"Field(s) {', '.join(invalid_fields)} is(are) not a valid field(s) in {cls.__name__}.")
    #         if 'timeframe' in kwargs.keys():
    #             timeframe = kwargs['timeframe']
    #             for key, value in kwargs:
    #                 if key not in ['date', 'timeframe']:
    #                     # # check if the type of kwargs values match with appropriate  Series field dtype.
    #                     # expected_d_type = _all_annotations[key].__args__[0]
    #                     # if not isinstance(value, expected_d_type):
    #                     #     raise TypeError(f"Invalid type for field '{key}'. Expected {expected_d_type}, got {type(value)}.")
    #                     result.loc[{'timeframe': timeframe, 'date': date, }, key] = value
    #         else:
    #             for key, value in kwargs.items():
    #                 if key not in ['date', 'timeframe']:
    #                     # # check if the type of kwargs values match with appropriate  Series field dtype.
    #                     # expected_d_type = _all_annotations[key].__args__[0]
    #                     # if not isinstance(value, expected_d_type):
    #                     #     raise TypeError(f"Invalid type for field '{key}'. Expected {expected_d_type}, got {type(value)}.")
    #                     result.loc[date, key] = value
    #         result = cls.cast_and_validate(result)
    #     return result
    #
    # @classmethod
    # def zz_cast_and_validate(cls: Type['ExpandedDf'], instance: Union['ExpandedDf', pd.DataFrame],
    #                          inplace: bool = True) -> 'ExpandedDf':
    #     result: 'ExpandedDf' = cast_and_validate(instance, cls.schema_data_frame_model)
    #     if inplace:
    #         instance.__dict__ = result.__dict__
    #         return instance
    #     else:
    #         return result

    @classmethod
    def read_file(cls, date_range_str: str, data_frame_type: str, generator: Callable,
                  skip_rows=None, n_rows=None, file_path: str = config.path_of_data,
                  zero_size_allowed: Union[None, bool] = None) -> 'BasePanderaDFM':
        """
        Read data from a file and return a DataFrame. If the file does not exist or the DataFrame does not
        match the expected columns, the generator function is used to create the DataFrame.

        This function reads data from a file with the specified `data_frame_type` and `date_range_str` parameters.
        If the file exists and the DataFrame columns match the expected columns defined in the configuration, the
        function returns the DataFrame. Otherwise, the `generator` function is invoked to generate the DataFrame,
        and then the columns of the generated DataFrame are checked against the expected columns.

        Parameters:
            date_range_str (str): The date range string used to construct the filename.
            data_frame_type (str): The type of DataFrame to read, e.g., 'ohlcva', 'multi_timeframe_ohlcva', etc.
            generator (Callable): The function that generates the DataFrame if needed.
            skip_rows (Optional[int]): The number of rows to skip while reading the file.
            n_rows (Optional[int]): The maximum number of rows to read from the file.
            file_path (str, optional): The path to the directory containing the data files.

        Returns:
            pd.DataFrame: The DataFrame read from the file or generated by the generator function.

        Raises:
            Exception: If the expected columns are not defined in the configuration or if the generated DataFrame
                       does not match the expected columns.

        Example:
            # Assuming you have a generator function 'generate_ohlcva' and 'ohlcva_columns' defined in configuration
            df = read_file(date_range_str='17-10-06.00-00T17-10-06', data_frame_type='ohlcva',
                           generator=generate_ohlcva)

        Note:
            This function first attempts to read the file based on the provided parameters. If the file is not found
            or the DataFrame does not match the expected columns, the generator function is called to create the DataFrame.
            :param zero_size_allowed:
            :param file_path:
            :param n_rows:
            :param skip_rows:
            :param date_range_str:
            :param data_frame_type:
            :param generator:
        """
        if date_range_str is None:
            date_range_str = config.processing_date_range
        df = None
        try:
            df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
        except FileNotFoundError as e:
            pass
        except Exception as e:
            raise e

        if zero_size_allowed is None:
            zero_size_allowed = after_under_process_date(date_range_str)
        if df is None or not cls.cast_and_validate(df, return_bool=True, zero_size_allowed=zero_size_allowed):
            generator(date_range_str)
            df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
            df = cls.cast_and_validate(df, zero_size_allowed=zero_size_allowed)
        else:
            df = cls.cast_and_validate(df, zero_size_allowed=zero_size_allowed)
        if datarange_is_not_cachable(date_range_str):
            os.remove(os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip'))
        return df

    @classmethod
    def concat(cls: Type['BasePanderaDFM'], left, right) -> 'BasePanderaDFM':
        # todo: test

        result: 'BasePanderaDFM' = concat(left, right)
        return result
