//! FRTB job entry point
pub mod prelude;
use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static!(
    pub static ref MEASURE_COL_MAP: HashMap<String, [&'static str; 11]> = HashMap::from(
        [
            ("Delta".to_string(), ["DeltaSpot", "Delta0.25", "Delta0.5Y", "Delta1Y",
             "Delta2Y", "Delta3Y", "Delta5Y", "Delta10Y", "Delta15Y", "Delta20Y", "Delta30Y"])
        ]
    );
);

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}