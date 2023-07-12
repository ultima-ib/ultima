import unittest

import polars as pl

import ultibi as ul

class TestCreationWithScan(unittest.TestCase):
    # TODO add tast for Scan
    def test_scan(self) -> None:
        scan = pl.scan_csv("../frtb_engine/data/frtb/Delta.csv", 
                           dtypes={"SensitivitySpot": pl.Float64})
        ul.DataSource.scan(scan)
    
    def test_inmemory(self) -> None:
        scan = pl.read_csv("../frtb_engine/data/frtb/Delta.csv", 
                           dtypes={"SensitivitySpot": pl.Float64})
        ul.DataSource.inmemory(scan)

if __name__ == "__main__":
    unittest.main()