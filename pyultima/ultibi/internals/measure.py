from typing import Type, Protocol
from polars import Series, DataType
import ultibi.internals as uli

from ..rust_module.ultima_pyengine import MeasureWrapper

class PyCalculator(Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(self, srs: list[Series], kwargs: dict[str, str]) -> int:
        ...

class Measure:
    inner: MeasureWrapper

    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 depends_upon: list[tuple[str, str]],
                 aggregation_restriction: str, 
                 precompute_filter: list[list[Type[uli.Filter]]]) -> None:
        pass

class BaseMeasure(Measure):
    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 precompute_filter: list[list[Type[uli.Filter]]],
                 aggregation_restriction: str, 
                 ) -> None:
        MeasureWrapper.new_basic(
            measure_name,
            calc,
            calc_output,
            input_cols,
            returns_scalar,
            [[y.inner for y in x] for x in precompute_filter],
            aggregation_restriction
        )

class DependantMeasure(Measure):
    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 depends_upon: list[tuple[str, str]]) -> None:
        pass