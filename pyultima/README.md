<br>

<p align="center">
    <a href="https://ultimabi.uk/" target="_blank">
    <img width="900" src="https://ultima-bi.s3.eu-west-2.amazonaws.com/imgs/logo.png" alt="Ultima Logo">
    </a>
</p>
<br>

<h3 align="center">the ultimate data analytics tool <br> for no code visualisation and collaborative exploration.</h3>

<h3 align="center">Present easier. &nbsp; Dig deeper. &nbsp; Review together. &nbsp;</h3>

# The Ultimate BI tool
With `Ultibi` you can turn your `DataFrame` into a pivot table with a UI and share it across organisation. You can also define measures applicable to your `DataFrame`.  This means your colleagues/consumers don't have to write any code to analyse the data.

<br>

<p align="center">
    <a href="https://frtb.demo.ultimabi.uk/" target="_blank">
    <img width="900" src="https://ultima-bi.s3.eu-west-2.amazonaws.com/imgs/UltimaScreenshotExplained.jpg" alt="Ultima Logo">
    </a>
</p>

<br>

Ultibi leverages on the giants: [Actix](https://github.com/actix/actix-web), [Polars](https://github.com/pola-rs/polars) and Rust which make this possible. We use TypeScript for the frontend.

# Examples

Our userguide is under development.
In the mean time refer to FRTB [userguide](https://ultimabi.uk/ultibi-frtb-book/).

## Python

### Quickstart
```python
import ultibi as ul
import polars as pl
import os
os.environ["RUST_LOG"] = "info" # enable logs
os.environ["ADDRESS"] = "0.0.0.0:8000" # host on this address

# Read Data
# for more details: https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html
df = pl.read_csv("titanic.csv")

# Convert it into an Ultibi DataSet
ds = ul.DataSet.from_frame(df)

# By default (might change in the future)
# Fields are Utf8 (non numerics) and integers
# Measures are numeric columns.
ds.ui() 
```

Then navigate to `http://localhost:8000` or checkout `http://localhost:8000/swagger-ui` for the OpenAPI documentation.

### More flexible - custom measures
```python
import ultibi as ul
import polars as pl
import os
os.environ["RUST_LOG"] = "info" # enable logs
os.environ["ADDRESS"] = "0.0.0.0:8000" # host on this address

# Read Data
# for more details: https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html
df = pl.read_csv("titanic.csv")

# Let's add some Custom/Bespoke Calculations to our UI

# Standard Calculator
def survival_mean_age(kwargs: dict[str, str]) -> pl.Expr:
    """Mean Age of Survivals
    pl.col("survived") is 0 or 1
    pl.col("age") * pl.col("survived") - age of survived person, otherwise 0
    pl.col("survived").sum() - number of survived
    """
    return pl.col("age") * pl.col("survived") / pl.col("survived").sum()

# Also a Standard Calculator
def example_dep_calc(kwargs: dict[str, str]) -> pl.Expr:
    return pl.col("SurvivalMeanAge_sum") + pl.col("SouthamptonFareDivAge_sum")

# When we need more involved calculations we go for a Custom Calculator
def custom_calculator(
            srs: list[pl.Series], kwargs: dict[str, str]
        ) -> pl.Series:
        """
        Southampton Fare/Age*multiplier
        """
        df = pl.DataFrame({"age": srs[0], 
                           "fare": srs[1], 
                           "e": srs[2]}) 
        # Add Indicator Column for Southampton
        df = df.with_columns(pl.when(pl.col("e")=="S").then(1).otherwise(0).alias("S")) 
        multiplier = float(kwargs.get("multiplier", 1))
        res = df["S"] * df["fare"] / df["age"] * multiplier
        return res

# inputs for the custom_calculator srs param
inputs = ["age", "fare", "embarked"]
# We return Floats
res_type = pl.Float64
# We return a Series, not a scalar (which otherwise would be auto exploded)
returns_scalar = False

measures = [
            ul.BaseMeasure(
                "SouthamptonFareDivAge",
                ul.CustomCalculator(
                    custom_calculator, res_type, inputs, returns_scalar
                ),
                # (Optional) - we are only interested in Southampton, so
                # unless other measures requested we might as well filter for Southampton only
                # However, if if multiple measures requested, their precompute_filters will be joined as OR.
                [[ul.EqFilter("embarked", "S")]],
                # PARAMS tab of the UI
                calc_params=[ul.CalcParam("mltplr", "1", "float")]
            ),
            ul.BaseMeasure(
                "SurvivalMeanAge",
                ul.StandardCalculator(survival_mean_age),
                aggregation_restriction="sum",
            ),
            ul.DependantMeasure(
                "A_Dependant_Measure",
                ul.StandardCalculator(example_dep_calc),
                [("SurvivalMeanAge", "sum"), ("SouthamptonFareDivAge", "sum")],
            ),
        ]

# Convert it into an Ultibi DataSet
ds = ul.DataSet.from_frame(df, bespoke_measures=measures)

# By default (might change in the future)
# Fields are Utf8 (non numerics) and integers
# Measures are numeric columns.
ds.ui() 
```

### DataSources
We provide aim to support different sources of the data. 
```python
scan = pl.read_csv("../frtb_engine/data/frtb/Delta.csv", 
                           dtypes={"SensitivitySpot": pl.Float64})
dsource = ul.DataSource.inmemory(scan)
ds = ul.DataSet.from_source(dsource)
ds.prepare() # .prepare() is only relevant to FRTB dataset currently
ds.ui()
```
If you don't want to/can't hold all your data in the process memory, you can sacrifise performance for memory with Scan/DataBase
```python
import polars as pl
import ultibi as ul
# Note that the LazyFrame query must start with scan_
# and must've NOT been collected
scan = pl.scan_csv("../frtb_engine/data/frtb/Delta.csv", 
                    dtypes={"SensitivitySpot": pl.Float64})
dsource = ul.DataSource.scan(scan)
ds = ul.DataSet.from_source(dsource)
ds.ui()
```

Note: Naturally the later two options will be slower, because prior to computing your measures we will need to read the relevant bits of the data into the process memory. 

### FRTB SA
[FRTB SA](https://en.wikipedia.org/wiki/Fundamental_Review_of_the_Trading_Book) is a great usecase for `ultibi`. FRTB SA is a set of standardised, computationally intensive rules established by the regulator. High business impact of these rules manifests in need for **analysis** and **visibility** thoroughout an organisation. Note: Ultima is not a certified aggregator. Always benchmark the results against your own interpretation of the rules.
See python frtb [userguide](https://ultimabi.uk/ultibi-frtb-book/).

### License 
`ultibi` python library is made available exclusively for the purpose of demonstrating the possibilities offered by the Software so users can evaluate the possibilities and potential of the Software. You may use the library for non comercial, non for profit activity. You may not use this library in production, but you can purchase an alternative license for doing so. 

Our `licenses` are extremely affordable, and you can check out the options by reaching out to us directly, via anatoly at ultimabi dot uk.