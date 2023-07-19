import unittest

import polars as pl

import ultibi as ul


class TestCreationWithScan(unittest.TestCase):
    # TODO add tast for Scan
    def test_scan(self) -> None:
        # Note that the LazyFrame query must start with scan_
        # and must've NOT been collected
        scan = pl.scan_csv(
            "../frtb_engine/data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64}
        )
        dsource = ul.DataSource.scan(scan)
        ds = ul.DataSet.from_source(dsource)
        self.assertRaises(ul.internals.UltimaError, ds.prepare)

    def test_inmemory(self) -> None:
        scan = pl.read_csv(
            "../frtb_engine/data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64}
        )
        dsource = ul.DataSource.inmemory(scan)
        ds = ul.DataSet.from_source(dsource)
        ds.prepare()


if __name__ == "__main__":
    unittest.main()
