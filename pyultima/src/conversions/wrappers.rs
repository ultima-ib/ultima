use once_cell::sync::Lazy;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    types::PyModule,
    FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python, ToPyObject,
};
use ultibi::polars::datatypes::DataType;

pub(crate) static POLARS: Lazy<PyObject> =
    Lazy::new(|| Python::with_gil(|py| PyModule::import(py, "polars").unwrap().to_object(py)));

#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl ToPyObject for Wrap<DataType> {
    fn to_object(&self, py: Python) -> PyObject {
        let ul = POLARS.as_ref(py);

        match &self.0 {
            DataType::Int8 => ul.getattr("Int8").unwrap().into(),
            DataType::Int16 => ul.getattr("Int16").unwrap().into(),
            DataType::Int32 => ul.getattr("Int32").unwrap().into(),
            DataType::Int64 => ul.getattr("Int64").unwrap().into(),
            DataType::UInt8 => ul.getattr("UInt8").unwrap().into(),
            DataType::UInt16 => ul.getattr("UInt16").unwrap().into(),
            DataType::UInt32 => ul.getattr("UInt32").unwrap().into(),
            DataType::UInt64 => ul.getattr("UInt64").unwrap().into(),
            DataType::Float32 => ul.getattr("Float32").unwrap().into(),
            DataType::Float64 => ul.getattr("Float64").unwrap().into(),
            DataType::Boolean => ul.getattr("Boolean").unwrap().into(),
            DataType::Utf8 => ul.getattr("Utf8").unwrap().into(),
            _ => PyTypeError::new_err("unsupported type").into_py(py),
        }
    }
}

impl FromPyObject<'_> for Wrap<DataType> {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let type_name = ob.get_type().name()?;

        let dtype = match type_name {
            "DataTypeClass" => {
                // just the class, not an object
                let name = ob.getattr("__name__")?.str()?.to_str()?;
                match name {
                    "UInt8" => DataType::UInt8,
                    "UInt16" => DataType::UInt16,
                    "UInt32" => DataType::UInt32,
                    "UInt64" => DataType::UInt64,
                    "Int8" => DataType::Int8,
                    "Int16" => DataType::Int16,
                    "Int32" => DataType::Int32,
                    "Int64" => DataType::Int64,
                    "Utf8" => DataType::Utf8,
                    "Binary" => DataType::Binary,
                    "Boolean" => DataType::Boolean,
                    "Categorical" => DataType::Categorical(None),
                    "Date" => DataType::Date,
                    "Time" => DataType::Time,
                    "Float32" => DataType::Float32,
                    "Float64" => DataType::Float64,
                    "List" => DataType::List(Box::new(DataType::Boolean)),
                    "Null" => DataType::Null,
                    "Unknown" => DataType::Unknown,
                    _ => return Err(PyValueError::new_err("not a correct polars DataType")),
                }
            }

            _ => return Err(PyTypeError::new_err("Not supported type")),
        };
        Ok(Wrap(dtype))
    }
}
