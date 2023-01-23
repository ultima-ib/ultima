from __future__ import annotations

from typing import Any, Type, TypeVar

import polars as pl

import ultibi.internals as uli

from ..rust_module.ultima_pyengine import DataSetWrapper

# Create a generic variable that can be 'Parent', or any subclass.
DS = TypeVar("DS", bound="DataSet")


class DataSet:
    """
    Main DataSet class

    Holds data, optionally validates and prepares Data,
     and finally executes request.
    """

    ds: DataSetWrapper
    prepared: bool

    def __init__(self, ds: DataSetWrapper, prepared: bool = False) -> None:

        self._ds = ds
        self.prepared = prepared

        """All column which you can group by. Currently those are string 
            and bool columns
        """
        self.fields: list[str] = self._ds.fields()

        """{measureName: "aggtype restriction(if any, otherwise
            None)"}. If none, then you can use any of the availiable agg operations.
            Check :func:`~ultima.internals.aggregation_ops` for supported aggregation
             operations
        """
        self.measures: "dict[str, str | None]" = self._ds.measures()

        """parameters which you can pass to the Request for the given DataSet

        Returns:
            list[dict[str, str|None]]: List of {"name": parameter name to be
            passed to the request, "hint": type hint of the param}
        """
        self.calc_params: "list[dict[str, str|None]]" = self._ds.calc_params()

    @classmethod
    def from_config_path(cls: Type[DS], path: str) -> DS:
        """
        Reads path to <config>.toml
        Converts into DataSourceConfig
        Builds DataSet from DataSourceConfig

        Args:
            path (str): path to <config>.toml

        Returns:
            T: Self
        """
        return cls(DataSetWrapper.from_config_path(path), False)

    @classmethod
    def from_frame(
        cls: Type[DS],
        df: pl.DataFrame,
        measures: "list[str] | None" = None,
        build_params: "dict[str, str] | None" = None,
        prepared: bool = False,
    ) -> DS:
        """
        Build DataSet directly from df

        Args:
            cls (Type[T]): _description_
            df (polars.DataFrame): _description_
            measures (list[str], optional): Used as a constrained on measures.
                Defaults to all numeric columns in the dataframe.
            build_params (dict | None, optional): Params to be used in prepare. Defaults
             to None.
            prepared (bool, optional): Was your DataFrame already prepared? Defaults
            to False.

        Returns:
            T: Self
        """
        return cls(DataSetWrapper.from_frame(df, measures, build_params), prepared)

    def prepare(self) -> None:
        """Does nothing unless overriden. To be used for one of computations.
            eg Weights Assignments

        Raises:
            OtherError: Calling prepare on an already prepared dataset
        """
        if not self.prepared:
            self._ds.prepare()
            self.prepared = True
        else:
            raise uli.OtherError("Calling prepare on an already prepared dataset")

    def validate(self) -> None:
        """Raises:
           uli.NoDataError: Calling prepare on an already prepared dataset

        Note: If you can guarantee your particular calculation would not require
        the missing columns you can proceed at your own risk!
        """
        self._ds.validate()

    def frame(self) -> pl.DataFrame:
        vec_srs = self._ds.frame()
        return pl.DataFrame(vec_srs)

    def execute(self, req: "dict[Any, Any]|uli.AggRequest") -> pl.DataFrame:
        """Make sure that requested groupby and filters exist in self.columns,
        Make sure that requested measures exist in self.measures
        Make sure that aggregation type for a measure is selected properly

        Args:
            req (dict[Any, Any]|uli.AggRequest): Request, to be converted
            into AggRequest.

        Returns:
            pl.DataFrame: If your request and data were constructed properly.
        """

        if isinstance(req, dict):
            req = uli.AggRequest(req)

        return uli.execute_agg(req, self)


class FRTBDataSet(DataSet):
    """FRTB flavour of DataSet"""

    @classmethod
    def from_config_path(cls: Type[DS], path: str) -> DS:
        return cls(DataSetWrapper.frtb_from_config_path(path), False)

    @classmethod
    def from_frame(
        cls: Type[DS],
        df: pl.DataFrame,
        measures: "list[str] | None" = None,
        build_params: "dict[str, str] | None" = None,
        prepared: bool = False,
    ) -> DS:
        return cls(DataSetWrapper.frtb_from_frame(df, measures, build_params), prepared)
