import unittest

import polars as pl

import ultibi as ul


class TestCreation(unittest.TestCase):
    # TODO add tast for Scan
    def test_frtbds_from_config(self) -> None:
        dataset = ul.FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

        frame = dataset.frame()

        dataset.validate()

        # Has already been prepared via the config
        self.assertRaises(ul.internals.UltimaError, dataset.prepare)
        assert "jurisdiction" in [cp[0] for cp in dataset.calc_params]
        assert "FX Curvature KbMinus" in dataset.measures
        assert isinstance(frame, pl.DataFrame)
        assert "TradeId" in frame

    def test_ds_from_frame(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
        df = pl.DataFrame(data)
        ds = ul.DataSet.from_frame(df)
        ds.validate()

        expected = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]})
        assert ds.frame().frame_equal(expected)
        assert {"a", "b"}.issubset(ds.measures.keys())

    def test_ds_validate(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
        df = pl.DataFrame(data)
        ds = ul.FRTBDataSet.from_frame(df)
        self.assertRaises(ul.internals.UltimaError, ds.validate)

    # TODO this will be turned into prepare_for_each_request
    def test_ds_already_prepared(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
        df = pl.DataFrame(data)
        ds = ul.DataSet.from_frame(df)
        ds.validate()
        # self.assertRaises(ul.internals.OtherError, ds.prepare)


if __name__ == "__main__":
    unittest.main()
