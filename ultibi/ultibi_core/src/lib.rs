#![allow(clippy::type_complexity)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![doc(html_no_source)]

pub mod add_row;
pub mod aggregations;
pub mod cache;
mod datarequest;
pub mod dataset;
pub mod errors;
pub mod execution;
pub mod filters;
pub mod helpers;
pub mod io;
mod measure;
pub mod overrides;
pub mod prelude;
pub mod reports;

pub use crate::prelude::*;
