use std::sync::Arc;

use once_cell::sync::Lazy;
use ultibi::{
    reports::report::{GroupbyAggReport, Report},
    DataFrame,
};

pub(crate) static _EQ11: Lazy<GroupbyAggReport> = Lazy::new(|| GroupbyAggReport {
    name: "Equity Bucket 11".into(),
    fixed_fields: vec![],
    calculator: Arc::new(|_dfs: &[DataFrame]| Ok(Report::default())),
});
