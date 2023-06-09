use polars::prelude::PolarsError;
use std::fmt::{Debug, Formatter};
use thiserror::Error;

pub type UltiResult<T> = Result<T, UltimaErr>;

#[derive(Error)]
pub enum UltimaErr {
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

impl Debug for UltimaErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use UltimaErr::*;
        match self {
            Polars(err) => write!(f, "{err}"),
            SerdeJson(err) => write!(f, "Couldn't serialize string. Check format. {err}"),
            Other(err) => write!(f, "BindingsError: {err}"),
        }
    }
}
