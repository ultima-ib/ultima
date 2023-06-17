use std::sync::Arc;

use once_cell::sync::Lazy;
use ultibi::{reports::report::{Reporter, Report}, DataFrame};

pub(crate) static _EQ11: Lazy<Reporter> = Lazy::new(||
    Reporter{
        name: "Equity Bucket 11".into(),
        requests: vec![],
        calculator: Arc::new(
            |_dfs: &[DataFrame]|{
                Ok(Report::default())
            }
        )
    }
);