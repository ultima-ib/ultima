import unittest

import polars as pl

import ultibi as ul


class TestCreation(unittest.TestCase):
    def test_basic(self) -> None:

        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "d", "b"]}
        df = pl.DataFrame(data)
        
        def calculator(srs: list[pl.Series], kwargs: dict[str, str]):
            print("here")
            multiplier = kwargs.get("my_param", 1)
            res = srs[0]**srs[1]*multiplier
            return res
        
        res_type = pl.Float64
        inputs = ["a", "b"]
        returns_scalar = False
        precompute_filter = ul.NeqFilter("c", "b")

        measures = [ 
        ul.BaseMeasure(
           "TestMeasure",
           calculator,
           res_type,
           inputs,
           returns_scalar,
           [[precompute_filter]]
         ) , 
        ul.BaseMeasure(
           "TestMeasure1",
           calculator,
           res_type,
           inputs,
           returns_scalar,
           [[precompute_filter]]
         )
        ] 

        ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

        assert "TestMeasure" in ds.measures
        assert "TestMeasure1" in ds.measures
        

    def test_dependant(self) -> None:
        pass

if __name__ == "__main__":
    unittest.main()