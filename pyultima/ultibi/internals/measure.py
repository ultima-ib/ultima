# Note Measure is not ready for release. Blocked due to
# https://github.com/pola-rs/polars/issues/8039

from typing import Protocol, Type, TypeVar

from polars import DataType, Expr, Series

from ultibi.internals.filters import Filter

from ..rust_module.ultibi_engine import (
    CalcParamWrapper,
    CalculatorWrapper,
    MeasureWrapper,
)


class CalcParam:
    inner = CalcParamWrapper

    def __init__(
        self, name: str, default: "str|None" = None, type_hint: "str|None" = None
    ):
        self.inner = CalcParamWrapper(name, default, type_hint)


TFilter = TypeVar("TFilter", bound=Filter)


class CustomCalculatorType(Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(self, srs: list[Series], kwargs: dict[str, str]) -> Series:
        ...


class StandardCalculatorType(Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(self, kwargs: dict[str, str]) -> Expr:
        ...


class Calculator:
    inner: CalculatorWrapper

    def __init__(self, calc_wrapper: CalculatorWrapper) -> None:
        """
        Not to be called directly
        """
        self.inner = calc_wrapper


class CustomCalculator(Calculator):
    """
    Use StandardCalculator if you can.

    Since CustomCalculator locks GIL, therefore killing parallisation.

    Examples:
        See BaseMeasure, DependantMeasure
    """

    def __init__(
        self,
        calc: CustomCalculatorType,
        calc_output: Type[DataType],
        input_cols: list[str],
        returns_scalar: bool,
    ) -> None:
        calc_wrapper = CalculatorWrapper.custom(
            calc,
            calc_output,
            input_cols,
            returns_scalar,
        )
        super().__init__(calc_wrapper)


class StandardCalculator(Calculator):
    """
    Calculator which returns polars Expression.
    https://docs.rs/polars/latest/polars/prelude/enum.Expr.html
    Expression then Serialised into Rust code and therefore can be executed
    on multiple cores.
    Not every Expression can be serialised. AnonymousFunction for example cannot be

    If your expression can't be serialised - use CustomCalculator(locks GIL)

    Examples:
        See BaseMeasure, DependantMeasure
    """

    def __init__(self, calc: StandardCalculatorType) -> None:
        calc_wrapper = CalculatorWrapper.standard(
            calc,
        )
        super().__init__(calc_wrapper)


TCalculator = TypeVar("TCalculator", bound=Calculator)


class Measure:
    """
    Do not initialise directly. Use BaseMeasure or DependantMeasure instead
    """

    inner: MeasureWrapper

    def __init__(self, measure_wrapper: MeasureWrapper) -> None:
        """
        Not to be called directly
        """
        self.inner = measure_wrapper


class BaseMeasure(Measure):
    """
    Executed in .filter(precompute_filter) .groupby() .agg() context

    Args:
        measure_name (str): Name of your measure
        calc (TCalculator): _description_
        precompute_filter (list[list[TFilter]]): Automatically
            filters every time measure is calclated.
            !!! Inner elements are joined as OR, outer elements are joined as AND
            !!! If your request consist of several measures with different
                precomupte filters, they will be joined as OR,
            !!! Not to be confused with ComputeRequest filter
        aggregation_restriction (str | None, optional):
            eg. if your measure should only be aggregated as "scalar" or "sum"
        calc_params (list[CalcParam] | None, optional):
            Allows user to set calc_params (which are passed to calculators) via UI


    Examples
    --------
    Usage:

    >>> import ultibi as ul
    >>> import polars as pl
    >>> data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "d", "b"]}
    >>> df = pl.DataFrame(data)
    >>> def custom_calculator(
    ...     srs: list[pl.Series], kwargs: dict[str, str]
    ... ) -> pl.Series:
    ...     multiplier = float(kwargs.get("multiplier", 1))
    ...     res = srs[0] ** srs[1] * multiplier
    ...     return res
    >>> def standard_calculator(kwargs: dict[str, str]) -> pl.Expr:
    ...     return pl.col("TestMeasure1_sum").mul(2)
    >>> res_type = pl.Float64
    >>> inputs = ["a", "b"]
    >>> returns_scalar = False
    >>> precompute_filter = ul.NeqFilter("c", "b")
    >>> measures = [
    ...     BaseMeasure(
    ...         "TestMeasure",
    ...         ul.CustomCalculator(
    ...             custom_calculator, res_type, inputs, returns_scalar
    ...         ),
    ...         [[precompute_filter]],
    ...         calc_params=[ul.CalcParam("mltplr", "1", "float")],
    ...     ),
    ...     BaseMeasure(
    ...         "TestMeasure1",
    ...         ul.CustomCalculator(
    ...             custom_calculator, res_type, inputs, returns_scalar
    ...         ),
    ...         [[precompute_filter]],
    ...         calc_params=[ul.CalcParam("mltplr", "1", "float")],
    ...     ),
    ...     DependantMeasure(
    ...         "Dependant",
    ...         ul.StandardCalculator(standard_calculator),
    ...         [("TestMeasure1", "sum")],
    ...     ),
    ... ]
    >>> ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

    """

    def __init__(
        self,
        measure_name: str,
        calc: TCalculator,
        precompute_filter: "list[list[TFilter]]|None" = None,
        aggregation_restriction: "str|None" = None,
        calc_params: "list[CalcParam]|None" = None,
    ) -> None:
        if precompute_filter:
            precompute_filter_inner = [[y.inner for y in x] for x in precompute_filter]
        else:
            precompute_filter_inner = None
        if calc_params:
            calc_params_inner = [calc_param.inner for calc_param in calc_params]
        else:
            calc_params_inner = None

        measure_wrapper = MeasureWrapper.new_basic(
            measure_name,
            calc.inner,
            precompute_filter_inner,
            aggregation_restriction,
            calc_params_inner,
        )
        super().__init__(measure_wrapper)


class DependantMeasure(Measure):
    """
    Executed in .with_columns() context

    Examples
    --------
    Usage:

    >>> import ultibi as ul
    >>> import polars as pl
    >>> data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "d", "b"]}
    >>> df = pl.DataFrame(data)
    >>> def custom_calculator(
    ...     srs: list[pl.Series], kwargs: dict[str, str]
    ... ) -> pl.Series:
    ...     multiplier = float(kwargs.get("multiplier", 1))
    ...     res = srs[0] ** srs[1] * multiplier
    ...     return res
    >>> def standard_calculator(kwargs: dict[str, str]) -> pl.Expr:
    ...     return pl.col("TestMeasure1_sum").mul(2)
    >>> res_type = pl.Float64
    >>> inputs = ["a", "b"]
    >>> returns_scalar = False
    >>> precompute_filter = ul.NeqFilter("c", "b")
    >>> measures = [
    ...     BaseMeasure(
    ...         "TestMeasure",
    ...         ul.CustomCalculator(
    ...             custom_calculator, res_type, inputs, returns_scalar
    ...         ),
    ...         [[precompute_filter]],
    ...         calc_params=[ul.CalcParam("mltplr", "1", "float")],
    ...     ),
    ...     BaseMeasure(
    ...         "TestMeasure1",
    ...         ul.CustomCalculator(
    ...             custom_calculator, res_type, inputs, returns_scalar
    ...         ),
    ...         [[precompute_filter]],
    ...         calc_params=[ul.CalcParam("mltplr", "1", "float")],
    ...     ),
    ...     DependantMeasure(
    ...         "Dependant",
    ...         ul.StandardCalculator(standard_calculator),
    ...         [("TestMeasure1", "sum")],
    ...     ),
    ... ]
    >>> ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

    """

    def __init__(
        self,
        measure_name: str,
        calc: TCalculator,
        depends_upon: list[tuple[str, str]],
        calc_params: "list[CalcParam]|None" = None,
    ) -> None:
        if calc_params:
            calc_params_inner = [calc_param.inner for calc_param in calc_params]
        else:
            calc_params_inner = None

        measure_wrapper = MeasureWrapper.new_dependant(
            measure_name,
            calc.inner,
            depends_upon,
            calc_params_inner,
        )
        super().__init__(measure_wrapper)
