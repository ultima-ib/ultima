import unittest

import polars as pl

import ultibi as ul


class TestCreation(unittest.TestCase):
    def test_compute(self) -> None:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
        df = pl.DataFrame(data)
        ds = ul.DataSet.from_frame(df)

        # What do we want to calculate?
        request = dict(measures=[["a", "sum"]], groupby=["c"])

        result = ds.compute(request)

        expected = pl.DataFrame({"c": ["a", "b"], "a_sum": [3, 3]})

        self.assertTrue(result.frame_equal(expected))


if __name__ == "__main__":
    unittest.main()
