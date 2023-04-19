<br>

<p align="center">
    <a href="https://ultimabi.uk/" target="_blank">
    <img width="900" src="/img/logo.png" alt="Ultima Logo">
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
    <img width="900" src="/img/titanic_gif.gif" alt="Ultima Logo">
    </a>
</p>

<br>

Ultibi leverages on the giants: [Actix](https://github.com/actix/actix-web), [Polars](https://github.com/pola-rs/polars) and Rust which make this possible. We use TypeScript for the frontend.

# Examples

Our userguide is under development.

## Python

```python
import ultibi as ul
import polars as pl
import os
os.environ["RUST_LOG"] = "info" # enable logs
os.environ["ADDRESS"] = "0.0.0.0:8000" # host on this address

# (Optional) Define Bespoke Measures

# In this example we simply read a csv
# for more details: https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html
df = pl.read_csv("titanic.csv")

# Convert it into an Ultibi DataSet
ds = ul.DataSet.from_frame(df)

# By default (might change in the future)
# Fields are Utf8 (non numerics) and integers
# Measures are numeric columns. In Rust you can define your own measures
ds.ui()
```

### FRTB SA
[FRTB SA](https://en.wikipedia.org/wiki/Fundamental_Review_of_the_Trading_Book) is a great usecase for `ultibi`. FRTB SA is a set of standardised, computationally intensive rules established by the regulator. High business impact of these rules manifests in need for **analysis** and **visibility** thoroughout an organisation. Note: Ultima is not a certified aggregator. Always benchmark the results against your own interpretation of the rules.
See python frtb [userguide](https://ultimabi.uk/ultibi-frtb-book/).