use crate::prelude::*;

use ndarray::prelude::*;
use log::warn;
use serde::Deserialize;

/// Shifts 2D array by {int} to the right
/// Creates a new Array2 (essentially cloning)
#[allow(dead_code)]
pub(crate)fn shift_right_by(by: usize, a: &Array2<f64>) -> Array2<f64> {
    
    // if shift_by is > than number of columns
    let x: isize = ( by % a.len_of(Axis(1)) ) as isize;

    // if shift by 0 simply return the original
    if x == 0 {
        return a.clone()
    }
    // create an uninitialized array
    let mut b = Array2::uninit(a.dim());

    // x first columns in b are two last in a
    // rest of columns in b are the initial columns in a
    a.slice(s![.., -x..]).assign_to(b.slice_mut(s![.., ..x]));
    a.slice(s![.., ..-x]).assign_to(b.slice_mut(s![.., x..]));

    // Now we can promise that `b` is safe to use with all operations
    unsafe {
        b.assume_init()
    }
}

/// if CRR2 feature is not activated, this will return BCBS
/// if jurisdiction is not part of optional params or can't parse this will return BCBS
pub(crate) fn get_jurisdiction(op: &OCP) -> Jurisdiction {
    op.as_ref()
    .and_then(|map| map.get("jurisdiction"))
    .and_then(|x| x.parse::<Jurisdiction>().ok())
    //.unwrap()
    .unwrap_or_else(||{
        warn!("Jurisdiction defaulted to: BCBS");
        Jurisdiction::default()
    })
}

/// Limited to types which implement Copy because this should only be used for types like f64
pub(crate) fn get_optional_parameter<'a, T>(op: &'a OCP, param: &str, default: &T) -> T 
where T: Deserialize<'a> + Copy{
    op.as_ref()
    .and_then(|map| map.get(param))
    .and_then(|x| serde_json::from_str::<T>(x).ok())
    .unwrap_or_else(||{*default})
}

/// we need to assert vec length, no other way to do it than create a func for vec
pub(crate) fn get_optional_parameter_vec<'a, T>(op: &'a OCP, param: &str, default: &Vec<T>) -> Vec<T>
where T: Deserialize<'a> + Clone{
    op.as_ref()
    .and_then(|map| map.get(param))
    .and_then(|x| serde_json::from_str::<Vec<T>>(x).ok())
    .and_then(|v| if v.len()==default.len(){Some(v)} else {None})
    .unwrap_or_else(||{default.clone()})
}

/// we need to assert arr shape, so we have a separate func for arrs
pub(crate) fn get_optional_parameter_array<'a>(op: &'a OCP, param: &str, default: &Array2<f64>) -> Array2<f64>{
    op.as_ref()
    .and_then(|map| map.get(param))
    .and_then(|x| serde_json::from_str::<Array2<f64>>(x).ok())
    .and_then(|arr| if arr.shape()==default.shape(){Some(arr)} else {None})
    .unwrap_or_else(||{default.to_owned()})
}

/// Helper used to identify when to early return from the main charge function
/// This gives a view of intermediate results, such as Kb, Sb and SbAlt
pub(crate) enum ReturnMetric {
    CapitalCharge,
    Kb,
    Sb,
    SbAlt,
    /// Curvature
    KbPlus,
    KbMinus, 
}