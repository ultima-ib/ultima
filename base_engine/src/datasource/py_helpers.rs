use polars::prelude::*;
use polars_arrow::export::arrow;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};
use rayon::prelude::*;

pub fn main(bucket: String, files: Vec<String>) -> Vec<DataFrame> {
    pyo3::prepare_freethreaded_python();
    let test = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/datasource/s3_helpers.py"));

    files
        .into_par_iter()
        .map(|s|{
            let from_python = Python::with_gil(|py|-> PyResult<Vec<Series>> {
                
                let app: Py<PyAny> = PyModule::from_code(py, test, "", "")?
                    .getattr("s3_csv_to_lst_srs")?
                    .into();
            
                let r: Vec<Py<PyAny>> = app.call1(py, (bucket.clone(), s) )?.extract(py)?;
            
                let res = r.iter().map(|x|{
                    let s = x.as_ref(py);
                    py_series_to_rust_series(s)
                })
                .collect::<PyResult<Vec<Series>>>()?;
               Ok(res)
            });
        let srs = from_python.expect("Could not get series");
        let df = DataFrame::new(srs).expect("couldn't conver series into frame");
        df
        })
    .collect::<Vec<DataFrame>>()
}

pub fn py_series_to_rust_series(series: &PyAny) -> PyResult<Series> {
    // rechunk series so that they have a single arrow array
    let series = series.call_method0("rechunk")?;

    let name = series.getattr("name")?.extract::<String>()?;

    // retrieve pyarrow array
    let array = series.call_method0("to_arrow")?;

    // retrieve rust arrow array
    let array = array_to_rust(array)?;

    Series::try_from((name.as_str(), array)).map_err(|e| PyValueError::new_err(format!("{}", e)))
}

/// Take an arrow array from python and convert it to a rust arrow array.
/// This operation does not copy data.
fn array_to_rust(arrow_array: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(arrow::ffi::ArrowArray::empty());
    let schema = Box::new(arrow::ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const arrow::ffi::ArrowArray;
    let schema_ptr = &*schema as *const arrow::ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    arrow_array.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = arrow::ffi::import_field_from_c(schema.as_ref()).unwrap();
        let array = arrow::ffi::import_array_from_c(*array, field.data_type).unwrap();
        Ok(array.into())
    }
}