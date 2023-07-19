use std::fmt::{Debug, Formatter};
use std::io::Error;

use polars::prelude::PolarsError;
use pyo3::exceptions::PyException;
use pyo3::{create_exception, PyErr};
//use pyo3::prelude::*;
use thiserror::Error;
use ultibi::errors::UltimaErr;

#[derive(Error)]
pub enum PyUltimaErr {
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Ultima(#[from] UltimaErr),
    #[error("{0}")]
    Other(String),
}

impl std::convert::From<std::io::Error> for PyUltimaErr {
    fn from(value: Error) -> Self {
        PyUltimaErr::Other(format!("{value}"))
    }
}

impl std::convert::From<PyUltimaErr> for PyErr {
    fn from(err: PyUltimaErr) -> PyErr {
        //let default = || PyRuntimeError::new_err(format!("{:?}", &err));

        use PyUltimaErr::*;
        match &err {
            Polars(err) => UltiPolarsError::new_err(err.to_string()),
            // Polars(err) => match err {
            //     PolarsError::ColumnNotFound(name) => NotFoundError::new_err(name.to_string()),
            //     PolarsError::ComputeError(err) => ComputeError::new_err(err.to_string()),
            //     PolarsError::SchemaFieldNotFound(err) => {
            //         SchemaFieldNotFound::new_err(err.to_string())
            //     }
            //     PolarsError::StructFieldNotFound(err) => {
            //         StructFieldNotFound::new_err(err.to_string())
            //     }
            //     PolarsError::NoData(err) => NoDataError::new_err(err.to_string()),
            //     PolarsError::ShapeMismatch(err) => ShapeError::new_err(err.to_string()),
            //     PolarsError::SchemaMismatch(err) => SchemaError::new_err(err.to_string()),
            //     PolarsError::Io(err) => PyIOError::new_err(err.to_string()),
            //     PolarsError::ArrowError(err) => ArrowErrorException::new_err(format!("{err}")),
            //     PolarsError::Duplicate(err) => DuplicateError::new_err(err.to_string()),
            //     PolarsError::InvalidOperation(err) => {
            //         InvalidOperationError::new_err(err.to_string())
            //     },
            //     PolarsError::StringCacheMismatch(err) => Other(err.to_string())
            //},
            SerdeJson(err) => SerdeJsonError::new_err(format!(
                "Couldn't (de)serialise input. Check format. {err}"
            )),
            Ultima(err) => UltimaError::new_err(err.to_string()),
            Other(_str) => OtherError::new_err(_str.to_string()),
        }
    }
}

impl Debug for PyUltimaErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use PyUltimaErr::*;
        match self {
            Polars(err) => write!(f, "{err}"),
            SerdeJson(err) => write!(f, "Couldn't serialize string. Check format. {err}"),
            Ultima(err) => write!(f, "Ultima error. {err}"),
            Other(err) => write!(f, "BindingsError: {err}"),
        }
    }
}

create_exception!(exceptions, NotFoundError, PyException);
create_exception!(exceptions, ComputeError, PyException);
create_exception!(exceptions, NoDataError, PyException);
create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, ShapeError, PyException);
create_exception!(exceptions, SchemaError, PyException);
create_exception!(exceptions, DuplicateError, PyException);
create_exception!(exceptions, InvalidOperationError, PyException);
create_exception!(exceptions, SerdeJsonError, PyException);
create_exception!(exceptions, OtherError, PyException);
create_exception!(exceptions, SchemaFieldNotFound, PyException);
create_exception!(exceptions, StructFieldNotFound, PyException);
create_exception!(exceptions, UltimaError, PyException);
create_exception!(exceptions, UltiPolarsError, PyException);
