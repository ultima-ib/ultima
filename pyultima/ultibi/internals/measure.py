from ..rust_module.ultima_pyengine import MeasureWrapper

class Measure:
    inner: MeasureWrapper

class BaseMeasure(Measure):
    pass

class DependantMeasure(Measure):
    pass