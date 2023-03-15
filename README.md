<br>

<p align="center">
    <a href="https://ultimabi.uk/" target="_blank">
    <img width="900" src="/img/logo.png" alt="Ultima Logo">
    </a>
</p>
<br>

<h3 align="center">the ultimate data analytics tool <br> for no code visualisation and collaborative exploration.</h3>

<h3 align="center">Present easier. &nbsp; Drill deeper. &nbsp; Review together. &nbsp;</h3>

# The Ultimate BI tool

Ultibi leverages on the giants: [Actix](https://github.com/actix/actix-web), [Polars](https://github.com/pola-rs/polars) who make this possible. We use TypeScript for the frontend.
<br>

<p align="center">
    <a href="https://frtb.demo.ultimabi.uk/" target="_blank">
    <img width="900" src="/img/titanic_gif.gif" alt="Ultima Logo">
    </a>
</p>

<br>

# Examples

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

```rust
//! Server side entry point
//! This to be conversted into server

use clap::Parser;
//use base_engine::prelude::*;
use driver::helpers::cli::CliOnce;

use std::sync::{Arc, RwLock};

use ultibi::{DataSet, VisualDataSet};
use ultibi::acquire::build_validate_prepare;

pub type DataSetType = frtb_engine::FRTBDataSet;

#[allow(clippy::uninlined_format_args)]
fn main() -> anyhow::Result<()> {
    // Read .env
    dotenv::dotenv().ok();

    let cli = CliOnce::parse();
    let setup_path = cli.config;

    // Build Data - here we build from config
    let data = build_validate_prepare::<DataSetType>(setup_path.as_str(),true, true);
    let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(data));
    
    // Assume non streaming mode
    // For more information see documentation
    ds.ui(false);

    Ok(())
}
```

`cargo run --release -- --config="frtb_engine/tests/data/datasource_config.toml"`

## Extending with your own data and measures
Currently possible in `Rust` only.
Implement `DataSet` or `CacheableDataSet` for your Struct. In particular, implement `get_measures` method.
See [frtb_engine](https://github.com/ultima-ib/ultima/tree/master/frtb_engine) and python frtb [userguide](https://ultimabi.uk/ultibi-frtb-book/)

## Bespoke Hosting
You don't have to use `.ui()`. You can write your own sevrer easily based on your needs (for example DB interoperability for authentication)
See an example [driver](https://github.com/ultima-ib/ultima/tree/master/driver)

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
`cargo run --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml" --requests="./driver/src/request.json"`

Similarly, for:
`cargo run --bin server --features FRTB_CRR2`
Although the meaning and usage of --requests is different here.

**NOTE**: frtb_engine/tests/data/datasource_config.toml is used by tests in frtb_engine crate. Therefore, the data paths (**files, attributes etc**) are "local" paths to frtb_engine. Either create your own config or change this one (never push changed to master though)

## Cli Parameters

Config is a set up for Data Source
Request is what you want to calculate
