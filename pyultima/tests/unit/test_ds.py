import polars as pl
from ultima.internals.dataset import FRTBDataSet, DataSet
import unittest

class TestCreation(unittest.TestCase):

    def test_frtbds_from_config(self) -> None:

        dataset = FRTBDataSet.from_config_path(
        "./tests/data/datasource_config.toml")

        frame = dataset.frame()
        assert isinstance(frame, pl.DataFrame)
        assert 'TradeId' in frame

        measures = dataset.measures()
        assert 'FX Curvature KbMinus' in measures

    def test_ds_from_frame(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
        df = pl.DataFrame(data)
        ds = DataSet.from_frame(df)

        expected = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]})
        assert ds.frame().frame_equal(expected)
        assert set(("a", "b")).issubset(ds.measures().keys())
    
if __name__ == '__main__':
    unittest.main()