use clap::Parser;

// TODO remove these default values
const CONFIG: &str = r"frtb_engine/data/frtb/datasource_config.toml";
pub const REQUESTS: &str = r"./driver/src/request.json";

/// Cli for one_off run
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct CliOnce {
    /// Sets a custom config file
    /// In future this to be a mandatory field
    #[arg(short, long, value_name = "PATH_TO_CONFIG_FILE", default_value_t = CONFIG.into())]
    pub config: String,
    /// Sets the request.json
    /// In future this to be a mandatory field
    #[arg(short, long, value_name = "PATH_TO_JSON_FILE", default_value_t = REQUESTS.into())]
    pub requests: String,
}

/// Cli for the server run
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct CliServer {
    /// Sets a custom config file
    /// In future this to be a mandatory field
    #[arg(short, long, value_name = "PATH_TO_CONFIG_FILE", default_value_t = CONFIG.into())]
    pub config: String,
    /// Sets the request.json
    #[arg(short, long, value_name = "PATH_TO_TEMPLATES_FILE")]
    pub requests: Option<String>,

    #[arg(short, long, value_name = "SOCKET_ADDRESS")]
    pub address: Option<String>,
}
