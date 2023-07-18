from __future__ import annotations

from typing import Any, Type, TypeVar, no_type_check

import polars as pl

import ultibi.internals as uli
from ultibi.internals.filters import AnyFilter
from ultibi.internals.measure import Measure

from ..rust_module.ultibi_engine import DataSetWrapper

TMeasure = TypeVar("TMeasure", bound=Measure)


# Create a generic variable that can be 'Parent', or any subclass.
DS = TypeVar("DS", bound="DataSet")


class DataSet:
    """
    Main DataSet class. Holds data source info, optionally validates and prepares Data,
     and executes request.

    Creation:
        Preffer `DataSet.from_source`
        `DataSet.from_config_path`
        `DataSet.from_frame`

    Examples
    --------
    Usage:

    >>> import ultibi as ul
    >>> import polars as pl
    >>> df = pl.DataFrame(
    ...     {
    ...         "a": [1, 2, -3],
    ...         "b": [4, 5, 6],
    ...         "c": ["z", "z", "w"],
    ...         "d": ["k", "y", "s"],
    ...     }
    ... )
    >>> ds = ul.DataSet.from_frame(df)
    >>> request = dict(
    ...     measures=[["a", "sum"], ["b", "max"]],
    ...     groupby=["c"],
    ...     filters=[  # c==a OR b in (5,6)
    ...         [
    ...             {"op": "Eq", "field": "c", "value": "z"},
    ...             {"op": "In", "field": "b", "value": ["6", "5"]},
    ...         ],
    ...         # AND k!=c
    ...         [{"op": "Neq", "field": "d", "value": "y"}],
    ...     ],
    ...     overrides=[
    ...         {
    ...             "field": "a",
    ...             "value": "100",
    ...             "filters": [
    ...                 [{"op": "Eq", "field": "b", "value": "4"}],
    ...             ],
    ...         }
    ...     ],
    ... )
    >>> result = ds.compute(request)

    """

    inner: DataSetWrapper

    def __init__(self, ds: DataSetWrapper) -> None:
        """
        Class constructor - not to br called directly.
        call .from_frame() or .from_config()
        """
        self.inner = ds

        """All column which you can group by. Currently those are string 
            and bool columns
        """
        self.fields: list[str] = self.inner.fields()

        """{measureName: "aggtype restriction(if any, otherwise
            None)"}. If none, then you can use any of the availiable agg operations.
            Check :func:`~ultima.internals.aggregation_ops` for supported aggregation
             operations
        """
        self.measures: "dict[str, str | None]" = self.inner.measures()

        """parameters which you can pass to the Request for the given DataSet

        Returns:
            list[dict[str, str|None]]: List of {"name": parameter name to be
            passed to the request, "hint": type hint of the param}
        """
        self.calc_params: "list[tuple[str, str|None, str|None]]" = (
            self.inner.calc_params()
        )

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
        return cls(DataSetWrapper.from_config_path(path))

    @classmethod
    def from_frame(
        cls: Type[DS],
        df: pl.DataFrame,
        measures: "list[str] | None" = None,
        build_params: "dict[str, str] | None" = None,
        bespoke_measures: "list[TMeasure] | None" = None,
    ) -> DS:
        """
        Build DataSet directly from df

        Args:
            cls (Type[T]): _description_
            df (polars.DataFrame): _description_
            measures (list[str], optional): Used as a constrained on which columns are
                measures.
                Defaults to all numeric columns in the dataframe.
            build_params (dict | None, optional): Params to be used in prepare. Defaults
             to None.
            prepared (bool, optional): Was your DataFrame already prepared? Defaults
            to True.

        Returns:
            T: Self
        """
        bespoke_measures = (
            [m.inner for m in bespoke_measures] if bespoke_measures else None
        )
        return cls(
            DataSetWrapper.from_frame(df, measures, build_params, bespoke_measures),
        )

    @classmethod
    def from_source(
        cls: Type[DS],
        ds: uli.DataSource,
        measures: "list[str] | None" = None,
        build_params: "dict[str, str] | None" = None,
        bespoke_measures: "list[TMeasure] | None" = None,
    ) -> DS:
        bespoke_measures = (
            [m.inner for m in bespoke_measures] if bespoke_measures else None
        )
        return cls(
            DataSetWrapper.from_source(
                ds.inner, measures, build_params, bespoke_measures
            ),
        )

    def prepare(self, collect: bool = True) -> None:
        """Does nothing unless overriden. To be used for one of computations.
            eg Weights Assignments
            #TODO throws exception if Scan or DB

        Args:
            collect (cool): non-lazy mode. Evaluates.

        Raises:
            OtherError: Calling prepare on an already prepared dataset
        """
        self.inner.prepare(collect)

    def validate(self) -> None:
        """Raises:
           uli.NoDataError: Calling prepare on an already prepared dataset

        Note: If you can guarantee your particular calculation would not require
        the missing columns you can proceed at your own risk!
        """
        self.inner.validate()

    @no_type_check
    def frame(self, fltrs: "list[list[AnyFilter]] | None" = None) -> pl.DataFrame:
        "Use with caution. The returned frame might be very large"
        if fltrs is not None:  # extract inner
            fltrs = [[a_fltr.inner for a_fltr in or_fltrs] for or_fltrs in fltrs]
        vec_srs = self.inner.frame(fltrs)
        return pl.DataFrame(vec_srs)

    def compute(self, req: "dict[Any, Any]|uli.ComputeRequest") -> pl.DataFrame:
        """Make sure that requested groupby and filters exist in self.columns,
        Make sure that requested measures exist in self.measures
        Make sure that aggregation type for a measure is selected properly

        Args:
            req (dict[Any, Any]|uli.ComputeRequest): Request, to be converted
                into AggRequest.
            streaming (bool): keep it as False unless you know what you are doing.
                If set to true, execute will try to .prepare() for each request,
                which will result in an error if Data has already been prepared.

        Returns:
            pl.DataFrame: If your request and data were constructed properly.
        """

        if isinstance(req, dict):
            req = uli.ComputeRequest(req)

        vec_srs = self.inner.compute(req._ar)

        return pl.DataFrame(vec_srs)

    def ui(self) -> None:
        """Spins up a localhost.
        You can control level of logging and the address like this:
        >>> import os
        >>> os.environ["RUST_LOG"] = "info"
        >>> os.environ["ADDRESS"] = "0.0.0.0:8000"
        """
        # Streaming mode calls prepare on each request
        # If already prepared we don't want to call it again
        self.inner.ui()


class FRTBDataSet(DataSet):
    """FRTB flavour of DataSet"""

    @classmethod
    def from_config_path(
        cls: Type[DS], path: str, bespoke_measures: "list[TMeasure] | None" = None
    ) -> DS:
        return cls(DataSetWrapper.frtb_from_config_path(path, bespoke_measures))

    @classmethod
    def from_frame(
        cls: Type[DS],
        df: pl.DataFrame,
        measures: "list[str] | None" = None,
        build_params: "dict[str, str] | None" = None,
        bespoke_measures: "list[TMeasure] | None" = None,
    ) -> DS:
        return cls(
            DataSetWrapper.frtb_from_frame(df, measures, build_params, bespoke_measures)
        )
