# Note Measure is not ready for release. Blocked due to
# https://github.com/pola-rs/polars/issues/8039

from typing import Protocol, Type, TypeVar

from polars import DataType, Expr, Series

from ultibi.internals.filters import Filter

from ..rust_module.ultima_pyengine import CalculatorWrapper, MeasureWrapper

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
    def __init__(self, calc: StandardCalculatorType) -> None:
        calc_wrapper = CalculatorWrapper.standard(
            calc,
        )
        super().__init__(calc_wrapper)


TCalculator = TypeVar("TCalculator", bound=Calculator)


class Measure:
    inner: MeasureWrapper

    def __init__(self, measure_wrapper: MeasureWrapper) -> None:
        """
        Not to be called directly
        """
        self.inner = measure_wrapper


class BaseMeasure(Measure):
    """
    Executed in .groupby() .agg() context
    """

    def __init__(
        self,
        measure_name: str,
        calc: TCalculator,
        # calc_output: Type[DataType],
        # input_cols: list[str],
        # returns_scalar: bool,
        precompute_filter: list[list[TFilter]],
        aggregation_restriction: str | None = None,
    ) -> None:
        measure_wrapper = MeasureWrapper.new_basic(
            measure_name,
            calc.inner,
            [[y.inner for y in x] for x in precompute_filter],
            aggregation_restriction,
        )
        super().__init__(measure_wrapper)


class DependantMeasure(Measure):
    """
    Executed in .with_columns() context
    """

    def __init__(
        self,
        measure_name: str,
        calc: TCalculator,
        depends_upon: list[tuple[str, str]],
    ) -> None:
        measure_wrapper = MeasureWrapper.new_dependant(
            measure_name, calc.inner, depends_upon
        )
        super().__init__(measure_wrapper)
