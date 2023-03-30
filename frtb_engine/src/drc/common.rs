// DRC
// as per 22.15
use polars::prelude::*;
use yearfrac::DayCountConvention;

/// Assumes existance of two columns: "COB" and "MaturityDate"
///
/// default format is "dd/mm/yyyy"
///
/// default DayCount Convention is Act360 (2 in Excel)
pub fn drc_scalinng(dc: Option<&String>, format: Option<&String>) -> Expr {
    let dc_int = dc.and_then(|x| x.parse::<u8>().ok()).unwrap_or(2);
    let dc = DayCountConvention::from_int(dc_int).expect("Couldn't parse Day count convention");
    //Not sure why compiler doesn't like this
    //let format = format.map(|x| &**x);
    let format = format.map(|x| x.to_owned());
    apply_multiple(
        move |columns| {
            let z = columns[0]
                .utf8()?
                .as_date(format.as_deref(), false)?
                .as_date_iter()
                .zip(
                    columns[1]
                        .utf8()?
                        .as_date(format.as_deref(), false)?
                        .as_date_iter(),
                )
                .map(|(x, y)| {
                    if let (Some(x), Some(y)) = (x, y) {
                        Some(dc.yearfrac(x, y))
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Some(z))
        },
        &[col("COB"), col("MaturityDate")],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}
