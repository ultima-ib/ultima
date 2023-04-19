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

# Read Data
# There are many many ways to create a Polars Dataframe. For example, you can go from Pandas:
# https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.from_pandas.html
# If youdo this consider using pandas' pyarrow backend https://datapythonista.me/blog/pandas-20-and-the-arrow-revolution-part-i
# For other options have a look at https://pola-rs.github.io/polars-book/user-guide/howcani/io/intro.html

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

## Rust
Note: currently if you want to use inbuild functionality of the .ui() method (instead of using a template like [template_drivers](https://github.com/ultima-ib/ultima/tree/master/template_drivers)) you have to
1. Build Frontend
```shell
cd frontend
npm run build
```
2. Set up env variable **STATIC_FILES_DIR**="{your_path}\frontend\dist"

3. 
```rust
use std::sync::Arc;
use std::sync::RwLock;

use polars::prelude::LazyCsvReader;
use ultibi::DataSet;
use ultibi::DataSetBase;
use ultibi::VisualDataSet;
use std::env;

pub fn example() {
    // See logs
    env::set_var("RUST_LOG", "info"); 

    // Read df
    let df = LazyCsvReader::new("titanic.csv")
        .finish()
        .unwrap();

    // Conver df into Arc<RwLock<ultibi::DataSet>>
    let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(
        DataSetBase::new(df, Default::default(), Default::default())
    ));

    // Visualise
    ds.ui(false);
}
```

`cargo run --release`

### FRTB SA
[FRTB SA](https://en.wikipedia.org/wiki/Fundamental_Review_of_the_Trading_Book) is a great usecase for `ultibi`. FRTB SA is a set of standardised, computationally intensive rules established by the regulator. High business impact of these rules manifests in need for **analysis** and **visibility** thoroughout an organisation. Note: Ultima is not a certified aggregator. Always benchmark the results against your own interpretation of the rules.
See python frtb [userguide](https://ultimabi.uk/ultibi-frtb-book/).

## Bespoke Hosting
You don't have to use `.ui()`. You can write your own sevrer easily based on your needs (for example DB interoperability for authentication)
See an example [template_drivers](https://github.com/ultima-ib/ultima/tree/master/template_drivers) bin server.

## How to build existing examples

`cargo build` or `cargo build --bin server`
After you've built,
Check out `target/debug/one_off.exe --help` for optional command line parameters.

With UI:
`cd frontend` and then `npm install & npm run build`
Then go back to `/ultima`

To run as a one off (run is a shortcut for build and execute):
`cargo run --features FRTB_CRR2 --release`
Which is equivallent to:
`cargo run --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml" --requests="./template_drivers/src/request.json"`

Similarly, for:
`cargo run --bin server --features FRTB_CRR2`
Although the meaning and usage of --requests is different here.

**NOTE**: frtb_engine/tests/data/datasource_config.toml is used by tests in frtb_engine crate. Therefore, the data paths (**files, attributes etc**) are "local" paths to frtb_engine. Either create your own config or change this one (never push changed to master though)

## Cli Parameters

Config is a set up for Data Source
Request is what you want to calculate
