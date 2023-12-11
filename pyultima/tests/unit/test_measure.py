import unittest

import polars as pl
from polars.testing import assert_series_equal

import ultibi as ul
from ultibi.internals.measure import BaseMeasure, DependantMeasure, RustCalculator


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
                calc_params=[ul.CalcParam("mltplr", "1", "float")],
            ),
            BaseMeasure(
                "TestMeasure1",
                ul.CustomCalculator(
                    custom_calculator, res_type, inputs, returns_scalar
                ),
                [[precompute_filter]],
                calc_params=[ul.CalcParam("mltplr", "1", "float")],
            ),
            DependantMeasure(
                "Dependant",
                ul.StandardCalculator(standard_calculator),
                [("TestMeasure1", "sum")],
            ),
        ]

        ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

        expected = [("mltplr", "float", "1")]

        assert "TestMeasure" in ds.measures
        assert "TestMeasure1" in ds.measures
        assert "Dependant" in ds.measures
        assert expected == ds.calc_params

        request = dict(
            measures=[["TestMeasure", "sum"]],
            groupby=["c"],
            calc_params=dict(multiplier="2"),
        )
        res = ds.compute(request)
        expected1 = pl.DataFrame({"c": ["a", "d"], "TestMeasure_sum": [2, 64]})
        self.assertTrue(res.equals(expected1))

        request = dict(
            measures=[["Dependant", "scalar"]],
            groupby=["c"],
            calc_params=dict(multiplier="2"),
        )
        res = ds.compute(request)
        expected2 = pl.DataFrame({"c": ["a", "d"], "Dependant": [4, 128]})
        self.assertTrue(res.equals(expected2))

    def test_rust_py_measure(self) -> None:
        import os
        import sys

        dir_path = os.path.dirname(os.path.realpath(__file__))

        if (
            "win" in sys.platform
        ):  # WINDOWS only, since pyd has ben compiled for windows
            lib = os.path.join(dir_path, "rust_py_measure.pyd")

            fo = dict(
                collect_groups="GroupWise",
                input_wildcard_expansion=False,
                returns_scalar=True,
                cast_to_supertypes=False,
                allow_rename=False,
                pass_name_to_apply=False,
                changes_length=False,
                check_lengths=True,
                allow_group_aware=True,
            )

            inputs = [pl.col("a"), pl.col("b")]

            calc = RustCalculator(lib, "hamming_distance", fo, inputs)

            mm = BaseMeasure("MyRustyMeasure", calc, aggregation_restriction="scalar")

            df = pl.DataFrame(
                {
                    "a": [1, 2, -3],
                    "b": [4, 5, 6],
                    "c": ["z", "z", "w"],
                    "d": ["k", "y", "s"],
                }
            )

            ds = ul.DataSet.from_frame(df, bespoke_measures=[mm])

            request = dict(measures=[["MyRustyMeasure", "scalar"]], groupby=["d"])
            result = ds.compute(request)

            assert_series_equal(
                result["MyRustyMeasure"], pl.Series("MyRustyMeasure", [1.0, 1.0, 1.0])
            )

            calc_params = dict(result="2")
            request = dict(
                measures=[["MyRustyMeasure", "scalar"]],
                groupby=["c"],
                calc_params=calc_params,
            )
            result = ds.compute(request)

            assert_series_equal(
                result["MyRustyMeasure"], pl.Series("MyRustyMeasure", [2.0, 2.0])
            )


if __name__ == "__main__":
    unittest.main()
