# Pre Requisites

Rust + Cargo (Rust's package manager) - follow [this link for installation instructions](https://doc.rust-lang.org/book/ch01-01-installation.html)

[npm](https://nodejs.org/en/download)

## How to build existing examples

`cd` into the root, then `cargo build --bin server_test`. This will compile the particular [server binary] (entry point: `.\template_drivers\src\bin\server.rs`). 
After you've built,
Check out `target/debug/server.exe --help` for optional **command line parameters**.

**!!!Note!!!** one of the parameters is `--config` which is defaulted to `"frtb_engine/data/datasource_config.toml"`. If you look inside this file, you will see paths relative to the `frtb_engine` path, like `"./data/frtb/Delta.csv"`. To make it work smoothly, there are two options:

1) Ammend paths in `frtb_engine/data/datasource_config.toml` to **full paths**. But you don't want to push these changes to Git. So you will need to exclude the file from git (without using .gitignore). [You can do it like this](https://stackoverflow.com/questions/71263349/vscode-ignore-files-without-using-gitignore).

2) Create your own datasource_config with **full paths** and make sure to pass it when invoking the binary (see examples below).

## UI:
`cd frontend` and then `npm install & npm run build`
Then go back to root (`/ultima`)

## Run:
`cargo run --bin server_test --features FRTB_CRR2 --release`
Which is equivallent to:
`cargo run --bin server_test --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml" --requests="./template_drivers/src/request.json"`
Remember to override `--config` if you need to.

## Run with watch for Frontend
When developing Frontend, it's usefull 
For that reason we have a special binary - **server** (*instead of server_test*):
-start the backend `cargo run --bin server --features FRTB_CRR2 --release -- --config="frtb_engine/tests/data/datasource_config.toml"`
-`cd frontend`, and then `npm run dev`. Logs will show a local host which will be automatically updated as soon as you change anything on the frontend.  


## Cli Parameters
`config` is a set up for Data Source
`requests` are templates, in case user doesn't want to recreate the same requests over and over again.