# Calculation

## Before you run

Before you specify what you want to calculate **make sure the measure/risk metric you are interested in is present in the dataset**:

```python
print( {{#include ./examples/frtb_calculate.py:5:5}} )
```

What you get there is a `dict`. The **keys** are measures' name. For example `SensitivitySpot` or `FX DeltaCharge Low`. The **items** indicate if there is any *restriction on aggregation method*, where `scalar` is a special method described below. If `None`, you are free to use any of the availiable:

```python
print( {{#include ./examples/frtb_calculate.py:6:6}} )
```

For example, `SensitivitySpot` can be aggregated any way you want for a given level, say Desk. In other wards, for a given Desk you can find the mean, max, min or sum (etc etc) of `SensitivitySpot`. On the other hand, you can not find mean, max, min (etc etc) of `FX DeltaCharge Low`. This wouldn't make sence since `FX DeltaCharge Low` is simply a single number defined by the regulation. Hence we call such aggregation method `scalar`.

Also, make sure that columns which appear in measures, grouby and filters are present as well.

## Run

Now we are good to form the request which we need. Say, we want to understand the `DRC` capital charge and all intermediate results, for every combination of `Desk` - `BucketBCBS`.

```python
{{#include ./examples/frtb_calculate.py:8:27}}
```

This has two parameter which we haven't talked about yet: hide_zeros and calc_params. hide_zeros simply removes rows where each measure is 0, and calc_params and few others we will talk about in [analysis](./whatif.md) chapter.

Convert the dict into Ultima's Aggregation Request. This helps validating that you've formed a legitimate request.

```python
{{#include ./examples/frtb_calculate.py:29:29}}
print(request)
```

Finally, let's execute it:

```python
{{#include ./examples/frtb_calculate.py:32:32}}
print(result)
print("Type: ", type(result))
```

Notice the returned object is a polars DataFrame. You can then do whatever you want with it. Print (to set how many columns you want to print make sure to use polars [config](https://pola-rs.github.io/polars/py-polars/html/reference/config.html)), or any on the [I/O](https://pola-rs.github.io/polars/py-polars/html/reference/io.html): save to csv, parquet, database etc.
