
<br>

<p align="center">
    <a href="https://ultimabi.uk/" target="_blank">
    <img width="900" src="/img/logo.png" alt="Ultima Logo">
    </a>
</p>
<br>

<h3 align="center">the ultimate data analytics tool <br> for no code visualisation and collaborative exploration.</h3>

<h3 align="center">Present easier. &nbsp; Analyse together. &nbsp; </h3>

# The Ultimate BI tool
Ultibi leverages on [Actix](https://github.com/actix/actix-web), [Polars](https://github.com/pola-rs/polars) and TypeScript for the frontend. 
<br>

<p align="center">
    <a href="https://frtb.demo.ultimabi.uk/" target="_blank">
    <img width="900" src="/img/titanic_gif.gif" alt="Ultima Logo">
    </a>
</p>

<br>

## Python
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

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;
#[global_allocator]
#[cfg(not(target_os = "linux"))]
static ALLOC: MiMalloc = MiMalloc;

pub type DataSetType = frtb_engine::FRTBDataSet;

#[allow(clippy::uninlined_format_args)]
fn main() -> anyhow::Result<()> {
    // Read .env
    dotenv::dotenv().ok();

    let cli = CliOnce::parse();
    let setup_path = cli.config;

    // Assume non streaming mode
    // For more information see documentation

    // Build Data
    let data = build_validate_prepare::<DataSetType>(setup_path.as_str(),true, true);
    let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(data));
    
    // Assume non streaming mode
    // For more information see documentation
    ds.ui(false);

    Ok(())
}
```
```cargo run --release -- --config="frtb_engine/tests/data/datasource_config.toml"```

# Examples
## Extending with your own measures
See [frtb_engine](https://github.com/ultima-ib/ultima/tree/master/frtb_engine) and python frtb [userguide](https://ultimabi.uk/ultibi-frtb-book/)
## Hosting
See [driver](https://github.com/ultima-ib/ultima/tree/master/driver) 

# Developers guide

## How to build
from ./ultima/
```cargo build``` or ```cargo build --bin server```

Check out ```target/debug/one_off.exe --help``` for optional command line parameters.

With UI:
```cd frontend``` and then ```npm install & npm run build```
Then go back to ```./ultima```

The default (run is a shortcult for build and run)
```cargo run --features FRTB_CRR2 --release```
is equivallent to:
```cargo run --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml" --requests="./driver/src/request.json"```

Similarly, for:
cargo run --bin server --features FRTB_CRR2
Although the meaning and usage of --requests is different here.

## Cli Parameters
Config is a set up for Data Source
Request is what you want to calculate

**NOTE**: frtb_engine/tests/data/datasource_config.toml is used by tests in frtb_engine crate. Therefore, the data paths (**files, attributes etc**) are "local" paths to frtb_engine. Either create your own config or change this one (never push changed to master though)
