use pyo3::{FromPyObject, PyAny, PyResult};
use ultibi::polars::lazy::dsl::Expr;
use ultibi::{polars::lazy::frame::LazyFrame, polars::prelude::DslPlan};

use crate::errors::PyUltimaErr;

pub struct PyLazyFrame(pub LazyFrame);

impl<'a> FromPyObject<'a> for PyLazyFrame {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s = ob.call_method0("__getstate__")?.extract::<Vec<u8>>()?;
        let lp: DslPlan = ciborium::de::from_reader(&*s).map_err(
            |e| PyUltimaErr::Other(
                format!("Error when deserializing LazyFrame. This may be due to mismatched polars versions. {}", e)
            )
        )?;
        Ok(PyLazyFrame(LazyFrame::from(lp)))
    }
}

pub struct PyExpr(pub Expr);

impl<'a> FromPyObject<'a> for PyExpr {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let b = ob.call_method0("__getstate__")?.extract::<Vec<u8>>()?;

        let e: Expr = ciborium::de::from_reader(b.as_slice()).map_err(|e| {
            PyUltimaErr::Other(format!("Error deserializing expression. This could be due to differenet Polars version. Try using Custom calculator. {}", e))
        })?;
        Ok(PyExpr(e))
    }
}
