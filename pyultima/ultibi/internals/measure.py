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
                 aggregation_restriction: str, 
                 precompute_filter: list[list[Type[uli.Filter]]]) -> None:
        pass

class DependantMeasure(Measure):
    def __init__(self, measure_name: str,
                 calc: PyCalculator, calc_output: DataType,
                 input_cols: list[str], returns_scalar: bool,
                 depends_upon: list[tuple[str, str]]) -> None:
        pass