import unittest

import polars as pl

import ultibi as ul


class TestFilters(unittest.TestCase):
    def test_ds_frame_fltr(self) -> None:
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]})
        ds = ul.DataSet.from_frame(df)

        from ultibi.internals.filters import Filter

        # inner filters are OR
        # outer filters are AND
        filter = Filter(dict(op="Eq", field="c", value="a"))
        filter2 = ul.EqFilter(field="b", value="4")
        filter3 = ul.NeqFilter(field="a", value="2")

        # (column c is "a" OR column b is 4) AND column a is not 2
        fltr = [[filter, filter2], [filter3]]

        expected = pl.DataFrame({"a": [1], "b": [4], "c": ["a"]})
        assert ds.frame(fltr).frame_equal(expected)


if __name__ == "__main__":
    unittest.main()
