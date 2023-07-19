import unittest

import polars as pl

import ultibi as ul


class TestCreation(unittest.TestCase):
    def test_compute(self) -> None:
        df = pl.DataFrame(
            {
                "a": [1, 2, -3],
                "b": [4, 5, 6],
                "c": ["z", "z", "w"],
                "d": ["k", "y", "s"],
            }
        )

        ds = ul.DataSet.from_frame(df)

        # Minimal required request of a compute
        request = dict(measures=[["a", "sum"]], groupby=["c"])
        result = ds.compute(request)
        expected = pl.DataFrame({"c": ["z", "w"], "a_sum": [3, -3]})
        self.assertTrue(result.frame_equal(expected))

        # more interesting example
        request = dict(
            measures=[["a", "sum"], ["b", "max"]],
            groupby=["c"],
            filters=[  # c==a OR b in (5,6)
                [
                    {"op": "Eq", "field": "c", "value": "z"},
                    {"op": "In", "field": "b", "value": ["6", "5"]},
                ],
                # AND k!=c
                [{"op": "Neq", "field": "d", "value": "y"}],
            ],
            overrides=[
                {
                    "field": "a",
                    "value": "100",
                    "filters": [
                        [{"op": "Eq", "field": "b", "value": "4"}],
                    ],
                }
            ],
        )
        result = ds.compute(request)
        expected = pl.DataFrame({"c": ["z", "w"], "a_sum": [100, -3], "b_max": [4, 6]})
        self.assertTrue(result.frame_equal(expected))


if __name__ == "__main__":
    unittest.main()
