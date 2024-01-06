from __future__ import annotations

import os
from typing import Type, Union, Callable

import pandas as pd
import pandera
from pandera import typing as pt

from Config import config
from helper.data_preparation import concat, read_with_timeframe, after_under_process_date, datarange_is_not_cachable, \
    index_names, column_dtypes
from helper.helper import log_d


class BasePanderaDFM(pandera.DataFrameModel):
    class Config:
        # to resolve pandera.errors.SchemaError: column ['XXXX'] not in dataframe
        add_missing_columns = True
        # to resolve pandera.errors.SchemaError: expected series ['XXXX'/None] to have type datetime64[ns, UTC]
        # , got object
        coerce = True

    # @classmethod
    # def empty(cls):
    #     if cls._sample_obj is None:
    #         raise Exception("_sample_obj should be set manually before.")
    #     return cls._sample_obj.drop(cls._sample_obj.index)


class ExtendedDf:
    schema_data_frame_model: BasePanderaDFM = None
    _sample_df: pd.DataFrame = None
    _empty_df: pd.DataFrame = None

    @classmethod
    def _set_index(cls, data: pt.DataFrame['BasePanderaDFM'], index_names) -> pt.DataFrame['BasePanderaDFM']:
        if 'date' not in data.columns:
            raise Exception(f"Expected to find 'date' in data")
        if hasattr(data.index, 'names') and 'timeframe' in data.index.names:
            if 'timeframe' not in data.columns:
                raise Exception(
                    f"'timeframe' is in the indexes of {cls.__name__}.{cls.schema_data_frame_model.__name__} "
                    f"so it is required!")
            return data.set_index(['timeframe', 'date'])
        else:
            return data.set_index(['date'])

    @classmethod
    def new(cls, dictionary_of_data: dict = None, strict: bool = True) -> pt.DataFrame['BasePanderaDFM']:
        if cls._sample_df is None:
            raise Exception(f"{cls}._sample_obj should be defined before!")
        if cls._empty_df is None:
            cls._empty_df = cls._sample_df.drop(cls._sample_df.index)
        if dictionary_of_data is not None:
            # to prevent ValueError: If using all scalar values, you must pass an index

            # non_list_keys = [key for key, value in dictionary_of_data.items() if not isinstance(value, list)]
            # if len(non_list_keys) > 0:
            #     raise Exception(f"Required to receive a dict of lists but {non_list_keys} are passing non-list values!")

            # _new = pd.DataFrame(dictionary_of_data)# pt.DataFrame[cls.schema_data_frame_model](dictionary_of_data)
            _new = cls._empty_df.copy()
            _index_names = index_names(cls._sample_df)
            index_tuple = tuple([dictionary_of_data[k] for k in dictionary_of_data.keys() if k in _index_names])
            _column_dtypes = column_dtypes(cls._sample_df, cls.schema_data_frame_model)
            unused_keys = []
            for key in dictionary_of_data.keys():
                if key in _column_dtypes.keys():
                    _new.loc[index_tuple, key] = dictionary_of_data[key]
                elif key not in _index_names:
                    unused_keys += [key]
            # _new = cls._set_index(_new, _index_names)
            # unused_keys = [key for key in dictionary_of_data.keys()
            #                if key not in cls._sample_df.columns and key not in ['date', 'timeframe']]
            if len(unused_keys) > 0:
                if strict:
                    raise Exception(f"Unused keys in the dictionary: {','.join(unused_keys)}")
            return _new
            # _index_names = index_names(cls._sample_df)
            # try:
            #     cls._empty_df = cls._set_index(cls._empty_df, _index_names)
            # except Exception as e:
            #     raise e
        _new = cls._empty_df.copy()
        return _new

    @classmethod
    def cast_and_validate(cls: Type['ExtendedDf'], df: Union['ExtendedDf', pd.DataFrame], return_bool: bool = False,
                          zero_size_allowed: bool = False, ) -> Union[pt.DataFrame['BasePanderaDFM'], bool]:
        if not zero_size_allowed:
            if len(df) == 0:
                raise Exception('Zero size data not allowed in parameters!')
        try:
            result = cls.schema_data_frame_model.validate(df)
        except pandera.errors.SchemaError as e:
            if return_bool:
                log_d(f"data is not valid according to {cls.schema_data_frame_model.__name__}")
                return False
            else:
                raise e
        if return_bool:
            return True
        return result

    @classmethod
    def read_file(cls, date_range_str: str, data_frame_type: str, generator: Callable,
                  skip_rows=None, n_rows=None, file_path: str = config.path_of_data,
                  zero_size_allowed: Union[None, bool] = None) -> pt.DataFrame['BasePanderaDFM']:
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
    def concat(cls, left: pt.DataFrame['BasePanderaDFM'], right: pt.DataFrame['BasePanderaDFM']) \
            -> pt.DataFrame['BasePanderaDFM']:
        result: pt.DataFrame['BasePanderaDFM'] = concat(left, right)
        return result
