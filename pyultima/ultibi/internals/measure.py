# Note Measure is not ready for release. Blocked due to
# https://github.com/pola-rs/polars/issues/8039

from typing import Type, Protocol
from polars import Series, DataType
from ultibi.internals.filters import Filter

from ..rust_module.ultima_pyengine import MeasureWrapper

class PyCalculator(Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(self, srs: list[Series], kwargs: dict[str, str]) -> int:
        ...

class Measure:
    inner: MeasureWrapper

    def __init__(self, measure_wrapper: MeasureWrapper) -> None:
        self.inner = measure_wrapper

class BaseMeasure(Measure):
    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 precompute_filter: list[list[Type[Filter]]],
                 aggregation_restriction: str|None = None, 
                 ) -> None:
        measure_wrapper = MeasureWrapper.new_basic(
            measure_name,
            calc,
            calc_output,
            input_cols,
            returns_scalar,
            [[y.inner for y in x] for x in precompute_filter],
            aggregation_restriction
        )
        super().__init__(measure_wrapper)

class DependantMeasure(Measure):
    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 depends_upon: list[tuple[str, str]]) -> None:
        pass