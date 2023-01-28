use std::collections::BTreeMap;

use polars::prelude::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::{CalcParameter, DataSourceConfig, MeasuresMap};

/// This is the default struct which implements Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Default)]
pub struct DataSetBase {
    /// Data
    pub frame: LazyFrame,
    /// Stores measures map, ie what you want to calculate
    pub measures: MeasuresMap,
    /// build_params are passed into .prepare()
    pub build_params: BTreeMap<String, String>,
}

/// The main Trait
///
/// If you have your own DataSet, implement this
pub trait DataSet: Send + Sync {
    /// Polars DataFrame clone is cheap:
    /// https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    fn get_lazyframe(&self) -> &LazyFrame;
    fn get_lazyframe_owned(self) -> LazyFrame;
    fn set_lazyframe(self, lf: LazyFrame) -> Self
    where
        Self: Sized;
    /// Modify lf in place
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame);

    fn get_measures(&self) -> &MeasuresMap;
    fn get_measures_owned(self) -> MeasuresMap;

    /// Cannot be defined since returns Self which is a Struct.
    /// Not possible to call [DataSet::new] either since it's not on self
    /// TODO create a From Trait
    fn from_config(conf: DataSourceConfig) -> Self
    where
        Self: Sized,
    {
        let (frame, measure_cols, build_params) = conf.build();
        let mm: MeasuresMap = MeasuresMap::from_iter(measure_cols);
        Self::new(frame, mm, build_params)
    }

    /// TODO remove this, this is not good for production
    fn from_config_for_tests(mut conf: DataSourceConfig, path_to_file_location: &str) -> Self
    where
        Self: Sized,
    {
        conf.change_path_on_abs_if_not_exist(path_to_file_location);
        let (frame, measure_cols, build_params) = conf.build();
        let mm: MeasuresMap = MeasuresMap::from_iter(measure_cols);
        Self::new(frame, mm, build_params)
    }

    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn new(frame: LazyFrame, mm: MeasuresMap, build_params: BTreeMap<String, String>) -> Self
    where
        Self: Sized;

    fn collect(self) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        Ok(self)
    }

    // These methods could be overwritten.

    /// Clones
    fn frame(&self) -> PolarsResult<DataFrame> {
        self.get_lazyframe().clone().collect()
    }

    /// Prepare runs BEFORE any calculations. In eager mode it runs ONCE
    /// Any pre-computations which are common to all queries could go in here.
    fn prepare(self) -> PolarsResult<Self>
    where
        Self: Sized,
    {
        let new_frame = self.prepare_frame(None)?;
        Ok(self.set_lazyframe(new_frame))
    }

    /// By returning a Frame this method can be used on a
    /// *lf - buffer. if None, function "prepares" self.lazy_frame()
    fn prepare_frame(&self, _lf: Option<LazyFrame>) -> PolarsResult<LazyFrame> {
        if let Some(lf) = _lf {
            Ok(lf)
        } else {
            Ok(self.get_lazyframe().clone())
        }
    }

    /// Calc params are used for the UI and hence are totally optional
    fn calc_params(&self) -> Vec<CalcParameter> {
        //self.get_measures()
        //    .iter()
        //    .map(|(name, measure)| measure.calc_params)
        vec![]
    }

    fn overridable_columns(&self) -> Vec<String> {
        self.get_lazyframe()
            .schema()
            .map(overrides_columns)
            .unwrap_or_default()
    }
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    fn validate_frame(&self, _: Option<&LazyFrame>, _: ValidateSet) -> PolarsResult<()> {
        Ok(())
    }
}

impl DataSet for DataSetBase {
    /// Polars DataFrame clone is cheap:
    /// https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    fn get_lazyframe(&self) -> &LazyFrame {
        &self.frame
    }
    fn get_lazyframe_owned(self) -> LazyFrame {
        self.frame
    }
    /// Modify lf in place
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame) {
        self.frame = lf;
    }
    fn set_lazyframe(self, lf: LazyFrame) -> Self {
        Self::new(lf, self.measures, self.build_params)
    }

    fn get_measures(&self) -> &MeasuresMap {
        &self.measures
    }
    fn get_measures_owned(self) -> MeasuresMap {
        self.measures
    }

    fn new(frame: LazyFrame, mm: MeasuresMap, build_params: BTreeMap<String, String>) -> Self {
        Self {
            frame,
            measures: mm,
            build_params,
        }
    }
    fn collect(self) -> PolarsResult<Self> {
        let lf = self.frame.collect()?.lazy();
        Ok(Self { frame: lf, ..self })
    }

    //    /// Validate Dataset contains columns
    //    /// files_join_attributes and attributes_join_hierarchy
    //    /// numeric_cols and TODO dimensions(groups and filters)
    //    /// !only! numeric_col can be a measure
    //    ///  and therefore <numeric_col> should be only one across DataSet
    //    /// see how to validate dtype:
    //    /// https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    //    fn validate(&self) {}
}

// TODO return Result
pub fn numeric_columns(schema: Arc<Schema>) -> Vec<String> {
    schema
        .iter_fields()
        .filter(|f| f.data_type().is_numeric())
        .map(|f| f.name)
        .collect::<Vec<String>>()
}

/// restrict columns which can be fields to Utf8 and Bool
pub fn fields_columns(schema: Arc<Schema>) -> Vec<String> {
    schema
        .iter_fields()
        .filter(|field| matches!(field.data_type(), DataType::Utf8))
        .map(|field| field.name)
        .collect::<Vec<String>>()
}

/// DataTypes supported for overrides are defined in [overrides::string_to_lit]
pub(crate) fn overrides_columns(schema: Arc<Schema>) -> Vec<String> {
    schema
        .iter_fields()
        .filter(|c| match c.data_type() {
            DataType::Utf8 | DataType::Boolean | DataType::Float64 => true,
            DataType::List(x) => {
                matches!(x.as_ref(), DataType::Float64)
            }
            _ => false,
        })
        .map(|c| c.name)
        .collect::<Vec<String>>()
}

impl Serialize for dyn DataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let measures = self
            .get_measures()
            .iter()
            .map(|(x, m)| (x, *m.aggregation()))
            .collect::<BTreeMap<&String, Option<&str>>>();

        let ordered_measures: BTreeMap<_, _> = measures.iter().collect();
        let utf8_cols = self
            .get_lazyframe()
            .schema()
            .map(fields_columns)
            .unwrap_or_default();
        let calc_params = self.calc_params();

        let mut seq = serializer.serialize_map(Some(4))?;

        seq.serialize_entry("fields", &utf8_cols)?;
        seq.serialize_entry("measures", &ordered_measures)?;
        seq.serialize_entry("calc_params", &calc_params)?;
        seq.end()
    }
}

pub enum ValidateSet {
    ALL,
    SUBSET1,
}
