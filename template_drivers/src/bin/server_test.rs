//! Example of a driver which calls .ui()

use clap::Parser;
//use base_engine::prelude::*;
use template_drivers::helpers::cli::CliOnce;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use ultibi::acquire::config_build_validate_prepare;
use ultibi::{DataSet, VisualDataSet};

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

// TODO is there a way to get rid of type definition here? In order to allow user to
// use bin without any need to modify the script? May be via features?
#[cfg(feature = "FRTB")]
pub type DataSetType = frtb_engine::FRTBDataSet;
#[cfg(not(feature = "FRTB"))]
pub type DataSetType = ultibi::DataSetBase;

#[allow(clippy::uninlined_format_args)]
fn main() -> anyhow::Result<()> {
    // Read .env
    dotenv::dotenv().ok();

    let cli = CliOnce::parse();
    let setup_path = cli.config;

    // Assume non streaming mode
    // For more information see documentation

    // Build Data
    let default = BTreeMap::new();
    let data = config_build_validate_prepare::<DataSetType>(setup_path.as_str(), default);
    let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(data));
    
    // Assume non streaming mode
    // For more information see documentation
    ds.ui();

    Ok(())
}
