import unittest

import polars as pl

import ultibi as ul
from ultibi.internals.measure import BaseMeasure


class TestCreation(unittest.TestCase):
    def test_basic(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "d", "b"]}
        df = pl.DataFrame(data)

        def calculator(srs: list[pl.Series], kwargs: dict[str, str]) -> pl.Series:
            print("here")
            multiplier = kwargs.get("my_param", 1)
            res = srs[0] ** srs[1] * multiplier
            return res

        res_type = pl.Float64
        inputs = ["a", "b"]
        returns_scalar = False
        precompute_filter = ul.NeqFilter("c", "b")

        measures = [
            BaseMeasure(
                "TestMeasure",
                calculator,
                res_type,
                inputs,
                returns_scalar,
                [[precompute_filter]],
            ),
            BaseMeasure(
                "TestMeasure1",
                calculator,
                res_type,
                inputs,
                returns_scalar,
                [[precompute_filter]],
            ),
        ]

        ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

        assert "TestMeasure" in ds.measures
        assert "TestMeasure1" in ds.measures

        request = dict(measures=[["TestMeasure", "sum"]], groupby=["c"])
        res = ds.compute(request)
        print(res)

    def test_dependant(self) -> None:
        pass


if __name__ == "__main__":
    unittest.main()
