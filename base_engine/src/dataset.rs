use std::collections::{BTreeMap, HashMap};

use polars::prelude::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::{derive_measure_map, DataSourceConfig, MeasuresMap};

/// This is the default struct which implements Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Default)]
pub struct DataSetBase {
    pub frame: LazyFrame,
    pub measures: MeasuresMap,
    /// build_params are used in .prepare()
    pub build_params: HashMap<String, String>,
}

/// This struct is purely for DataSet descriptive purposes.
/// Recall measure may take parameters in form of HashMap<paramName, paramValue>
/// This struct returns all possible paramNames for the given Dataset (for UI purposes only)
#[derive(Debug, Default, Clone, Serialize)]
pub struct CalcParameter {
    pub name: String,
    pub default: Option<String>,
    pub type_hint: Option<String>,
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
    fn get_measures(&self) -> &MeasuresMap;
    fn get_measures_owned(self) -> MeasuresMap;
    /// Modify lf in place
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame);

    // Cannot be defined since returns Self which is a Struct
    // TODO create a From Trait
    fn from_config(conf: DataSourceConfig) -> Self
    where
        Self: Sized;

    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn new(frame: LazyFrame, mm: MeasuresMap, build_params: HashMap<String, String>) -> Self
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
    /// *lf - biffer. if None, function "prepares" self.lazy_frame()
    fn prepare_frame(&self, _lf: Option<LazyFrame>) -> PolarsResult<LazyFrame> {
        if let Some(lf) = _lf {
            Ok(lf)
        } else {
            Ok(self.get_lazyframe().clone())
        }
    }

    /// Calc params are used for the UI and hence are totally optional
    fn calc_params(&self) -> Vec<CalcParameter> {
        vec![]
    }

    fn overridable_columns(&self) -> Vec<String> {
        overrides_columns(self.get_lazyframe())
    }
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    fn validate(&self) -> bool {
        true
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
    fn set_lazyframe(self, lf: LazyFrame) -> Self
    where
        Self: Sized,
    {
        Self {
            frame: lf,
            measures: self.measures,
            build_params: self.build_params,
        }
    }
    fn get_measures(&self) -> &MeasuresMap {
        &self.measures
    }
    fn get_measures_owned(self) -> MeasuresMap {
        self.measures
    }

    fn from_config(conf: DataSourceConfig) -> Self {
        let (frame, measure_cols, build_params) = conf.build();
        let mm: MeasuresMap = derive_measure_map(measure_cols);
        Self {
            frame,
            measures: mm,
            build_params,
        }
    }
    fn new(frame: LazyFrame, mm: MeasuresMap, build_params: HashMap<String, String>) -> Self {
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
pub(crate) fn numeric_columns(lf: &LazyFrame) -> Vec<String> {
    lf.schema().map_or_else(
        |_| vec![],
        |schema| {
            schema
                .iter_fields()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| f.name)
                .collect::<Vec<String>>()
        },
    )
}

// TODO return Result
pub(crate) fn utf8_columns(lf: &LazyFrame) -> Vec<String> {
    lf.schema().map_or_else(
        |_| vec![],
        |schema| {
            schema
                .iter_fields()
                .filter(|field| matches!(field.data_type(), DataType::Utf8))
                .map(|field| field.name)
                .collect::<Vec<String>>()
        },
    )
}

/// DataTypes supported for overrides are defined in [overrides::string_to_lit]
pub(crate) fn overrides_columns(lf: &LazyFrame) -> Vec<String> {
    //let mut res = vec![];
    lf.schema().map_or_else(
        |_| vec![],
        |schema| {
            let res = schema
                .iter_fields()
                .filter(|c| match c.data_type() {
                    DataType::Utf8 | DataType::Boolean | DataType::Float64 => true,
                    DataType::List(x) => {
                        matches!(x.as_ref(), DataType::Float64)
                    }
                    _ => false,
                })
                .map(|c| c.name)
                .collect::<Vec<String>>();
            res
        },
    )
}

impl Serialize for dyn DataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let measures = self
            .get_measures()
            .iter()
            .map(|(x, m)| (x, m.aggregation))
            .collect::<HashMap<&String, Option<&str>>>();

        let ordered_measures: BTreeMap<_, _> = measures.iter().collect();
        let utf8_cols = utf8_columns(self.get_lazyframe());
        let calc_params = self.calc_params();

        let mut seq = serializer.serialize_map(Some(4))?;

        seq.serialize_entry("fields", &utf8_cols)?;
        seq.serialize_entry("measures", &ordered_measures)?;
        seq.serialize_entry("calc_params", &calc_params)?;
        seq.end()
    }
}
