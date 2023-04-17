import unittest

import polars as pl

import ultibi as ul
from ultibi.internals.measure import BaseMeasure, DependantMeasure


class TestCreation(unittest.TestCase):
    def test_basic(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "d", "b"]}
        df = pl.DataFrame(data)

        def custom_calculator(
            srs: list[pl.Series], kwargs: dict[str, str]
        ) -> pl.Series:
            multiplier = float(kwargs.get("multiplier", 1))
            res = srs[0] ** srs[1] * multiplier
            return res

        def standard_calculator(kwargs: dict[str, str]) -> pl.Expr:
            return pl.col("TestMeasure1_sum").mul(2)

        res_type = pl.Float64
        inputs = ["a", "b"]
        returns_scalar = False
        precompute_filter = ul.NeqFilter("c", "b")

        measures = [
            BaseMeasure(
                "TestMeasure",
                ul.CustomCalculator(
                    custom_calculator, res_type, inputs, returns_scalar
                ),
                [[precompute_filter]],
            ),
            BaseMeasure(
                "TestMeasure1",
                ul.CustomCalculator(
                    custom_calculator, res_type, inputs, returns_scalar
                ),
                [[precompute_filter]],
            ),
            DependantMeasure(
                "Dependant",
                ul.StandardCalculator(standard_calculator),
                [("TestMeasure1", "sum")],
            ),
        ]

        ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

        assert "TestMeasure" in ds.measures
        assert "TestMeasure1" in ds.measures
        assert "Dependant" in ds.measures

        request = dict(
            measures=[["TestMeasure", "sum"]],
            groupby=["c"],
            calc_params=dict(multiplier="2"),
        )
        res = ds.compute(request)
        expected = pl.DataFrame({"c": ["a", "d"], "TestMeasure_sum": [2, 64]})
        self.assertTrue(res.frame_equal(expected))

        request = dict(
            measures=[["Dependant", "scalar"]],
            groupby=["c"],
            calc_params=dict(multiplier="2"),
        )
        res = ds.compute(request)
        expected = pl.DataFrame({"c": ["a", "d"], "Dependant": [4, 128]})
        self.assertTrue(res.frame_equal(expected))

    def test_dependant(self) -> None:
        pass


if __name__ == "__main__":
    unittest.main()
