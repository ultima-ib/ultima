# ultima
The Ultimate BI tool

# How to run
from ./ultima/
cargo build or cargo build --bin server

Check out target/debug/one_off.exe --help for optional command line parameters.

The default 
cargo run --features FRTB_CRR2 --release
is equivallent to:
cargo run --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml" --requests="./driver/src/request.json"

Similarly, for:
cargo run --bin server --features FRTB_CRR2
Although the meaning and usage of --requests is different here.

Config is a set up for Data Source
Request is what you want to calculate

NOTE: frtb_engine/tests/data/datasource_config.toml is used by tests in frtb_engine crate. Therefore, the data paths (files, attributes etc) are "local" paths to frtb_engine. Either create your own config or change this one (never push changed to master though)