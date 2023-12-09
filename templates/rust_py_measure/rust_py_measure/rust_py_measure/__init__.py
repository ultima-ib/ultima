import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location
from ultibi import RustCalculator, BaseMeasure

lib = _get_shared_lib_location(__file__)

fo = dict(
        collect_groups="GroupWise",
          input_wildcard_expansion=False,
          returns_scalar=True,
          cast_to_supertypes=False,
          allow_rename=False,
          pass_name_to_apply=False,
          changes_length=False,
          check_lengths = True,
          allow_group_aware=True
          )

inputs = [pl.col("a"), pl.col("b")]

calc = RustCalculator(lib, "hamming_distance", fo, inputs)

mm = BaseMeasure("MyRustyMeasure", calc, aggregation_restriction="scalar")

my_rusty_measures = [mm]