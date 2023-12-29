from __future__ import annotations

from typing import TypeVar, Type, Union

import pandas as pd

from Config import config
from Model.SignalDf import SignalDf
from data_preparation import empty_df, all_annotations, cast_and_validate, read_file, no_generator, concat
from pandera import typing as pt


class ExpandedDf:
    @classmethod
    def new(cls: Type['ExpandedDf'], **kwargs) -> 'ExpandedDf':
        if ((not hasattr(cls, 'schema_data_frame_model'))
                or (not issubclass(cls.schema_data_frame_model, pt.Dataframe))):
            raise Exception(
                f"{cls.__name__}.schema_data_frame_model should be defined as a subclass of pandera.typing.Dataframe before calling {cls.__name__}.new(...)")
        result = empty_df(cls.schema_data_frame_model)
        if len(kwargs) > 0:
            _all_annotations = all_annotations(cls.schema_data_frame_model)
            # # check if all Series fields of required self.schema_data_frame_model are present in kwargs
            # required_fields = set(_all_annotations.keys())
            # provided_fields = set(kwargs.keys())
            # missing_fields = required_fields - provided_fields
            # if missing_fields:
            #     raise ValueError(f"Missing required fields in kwargs: {missing_fields}")
            if not 'date' in kwargs.keys():
                raise Exception("'date' is the mandatory TimestampIndex and is required!")
            date = kwargs['date']
            # check if all of kwargs keys are in Series fields
            invalid_fields = [field for field in kwargs.keys() if field not in _all_annotations.keys()]
            if len(invalid_fields) > 0:
                raise ValueError(
                    f"Field(s) {', '.join(invalid_fields)} is(are) not a valid field(s) in {cls.__name__}.")
            if 'timeframe' in kwargs.keys():
                timeframe = kwargs['timeframe']
                for key, value in kwargs:
                    if key not in ['date', 'timeframe']:
                        # # check if the type of kwargs values match with appropriate  Series field dtype.
                        # expected_d_type = _all_annotations[key].__args__[0]
                        # if not isinstance(value, expected_d_type):
                        #     raise TypeError(f"Invalid type for field '{key}'. Expected {expected_d_type}, got {type(value)}.")
                        result.loc[{'timeframe': timeframe, 'date': date, }, key] = value
            else:
                for key, value in kwargs:
                    if key not in ['date', 'timeframe']:
                        # # check if the type of kwargs values match with appropriate  Series field dtype.
                        # expected_d_type = _all_annotations[key].__args__[0]
                        # if not isinstance(value, expected_d_type):
                        #     raise TypeError(f"Invalid type for field '{key}'. Expected {expected_d_type}, got {type(value)}.")
                        result.loc[date, key] = value
            result = cls.cast_and_validate(result)
        assert type(result) == cls.__name__
        return result

    @classmethod
    def cast_and_validate(cls: Type['ExpandedDf'], instance: Union['ExpandedDf', pd.DataFrame],
                          inplace: bool = True) -> 'ExpandedDf':
        result: 'ExpandedDf' = cast_and_validate(instance, SignalDf)
        if inplace:
            instance.__dict__ = result.__dict__
            return instance
        else:
            return result

    @classmethod
    def read(cls: Type['ExpandedDf'], date_range_str) -> 'ExpandedDf':
        if date_range_str is None:
            date_range_str = config.processing_date_range
        result: 'ExpandedDf' = read_file(date_range_str, 'signal', no_generator, cls)
        return result

    @classmethod
    def concat(cls: Type['ExpandedDf'], left, right) -> 'ExpandedDf':
        result: 'ExpandedDf' = concat(left, right)
        return result
