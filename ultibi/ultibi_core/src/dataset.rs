use std::collections::{BTreeMap, HashSet};

use polars::prelude::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::cache::{Cache, CacheableDataSet};
use crate::{derive_basic_measures_vec, execute, Measure, CPM};
use crate::{CalcParameter, ComputeRequest, DataSourceConfig, MeasuresMap};

/// This is the default struct which implements Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Default)]
pub struct DataSetBase {
    /// Data
    pub frame: LazyFrame,
    /// Stores measures map, ie what you want to calculate
    pub measures: MeasuresMap,
    /// build_params are passed into .prepare() - if streaming or add_row
    ///  this happens during the execution. Hence we can't remove and pass to .prepare() directly
    pub build_params: BTreeMap<String, String>,
    /// Cache
    pub cache: Cache,
}

/// The main Trait
///
/// If you have your own DataSet, implement this
pub trait DataSet: Send + Sync {
    #[cfg(feature = "ui")]
    fn ui(&self) {
        ultibi_server::run_server(self)
    }
    /// Polars DataFrame clone is cheap:
    /// https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    /// This method gets the main LazyFrame of the Dataset
    fn get_lazyframe(&self) -> &LazyFrame;

    /// Modify lf in place
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame);

    /// Get all measures associated with the DataSet
    fn get_measures(&self) -> &MeasuresMap;

    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn new(frame: LazyFrame, mm: MeasuresMap, params: CPM) -> Self
    where
        Self: Sized;

    /// Cannot be defined since returns Self which is a Struct.
    /// Not possible to call [DataSet::new] either since it's not on self
    fn from_config(conf: DataSourceConfig) -> Self
    where
        Self: Sized,
    {
        let (frame, measure_cols, bp) = conf.build();
        let mm: MeasuresMap = MeasuresMap::from_iter(measure_cols);
        Self::new(frame, mm, bp)
    }

    /// Either place your desired numeric columns and bespokes in
    /// *ms and set include_numeric_cols_as_measures = False
    /// or set your bespokes in *ms and include_numeric_cols_as_measures = True
    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn from_vec(
        frame: LazyFrame,
        mut ms: Vec<Measure>,
        include_numeric_cols_as_measures: bool,
        params: CPM,
    ) -> Self
    where
        Self: Sized,
    {
        if include_numeric_cols_as_measures {
            let num_cols = frame
                .schema()
                .map(numeric_columns)
                .expect("Failed to obtain numeric columns");

            let numeric_cols_as_measures = derive_basic_measures_vec(num_cols);
            ms.extend(numeric_cols_as_measures);
        }

        let mm: MeasuresMap = MeasuresMap::from_iter(ms);
        Self::new(frame, mm, params)
    }

    /// Collects the (main) LazyFrame of the DataSet
    fn collect(&mut self) -> PolarsResult<()>
    where
        Self: Sized,
    {
        let lf = self.get_lazyframe().clone().collect()?.lazy();
        self.set_lazyframe_inplace(lf);
        Ok(())
    }

    // These methods could be overwritten.

    /// Clones
    fn frame(&self) -> PolarsResult<DataFrame> {
        self.get_lazyframe().clone().collect()
    }

    /// Prepare runs BEFORE any calculations. In eager mode it runs ONCE
    /// Any pre-computations which are common to all queries could go in here.
    /// Calls [DataSet::prepare_frame] insternally
    fn prepare(&mut self) -> PolarsResult<()>
    where
        Self: Sized,
    {
        let new_frame = self.prepare_frame(None)?;
        self.set_lazyframe_inplace(new_frame);
        Ok(())
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
        let mut res = vec![];

        for measure in self.get_measures().values() {
            res.extend_from_slice(measure.calc_params())
        }

        let hash_res: HashSet<CalcParameter> = res.into_iter().collect();

        hash_res.into_iter().collect()
    }

    /// Limits overridable columns which you can override in
    /// See [AggregationRequest::overrides]
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

    /// * `streaming` - See polars streaming. Use when your LazyFrame is a Scan if you don't want to load whole frame
    /// into memory. See: https://www.rhosignal.com/posts/polars-dont-fear-streaming/
    fn compute(&self, r: ComputeRequest, streaming: bool) -> PolarsResult<DataFrame> {
        execute(self, r, streaming)
    }

    /// Indicates if your DataSet has a cache or not
    /// It is recommended that you implement CacheableDataSet
    /// make sure to return Some(&self)
    fn as_cacheable(&self) -> Option<&dyn CacheableDataSet> {
        None
    }
}

impl DataSet for DataSetBase {
    /// Polars DataFrame clone is cheap:
    /// https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    fn get_lazyframe(&self) -> &LazyFrame {
        &self.frame
    }
    /// Modify lf in place
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame) {
        self.frame = lf;
    }

    fn get_measures(&self) -> &MeasuresMap {
        &self.measures
    }

    //fn new(frame: LazyFrame, mm: MeasuresMap, build_params: BTreeMap<String, String>) -> Self {
    //    Self {
    //        frame,
    //        measures: mm,
    //        build_params,
    //        ..Default::default()
    //    }
    //}

    fn new(frame: LazyFrame, mm: MeasuresMap, build_params: CPM) -> Self {
        Self {
            frame,
            measures: mm,
            build_params,
            ..Default::default()
        }
    }

    //fn collect(self) -> PolarsResult<Self> {
    //    let lf = self.frame.collect()?.lazy();
    //    Ok(Self { frame: lf, ..self })
    //}

    //    /// Validate Dataset contains columns
    //    /// files_join_attributes and attributes_join_hierarchy
    //    /// numeric_cols and TODO dimensions(groups and filters)
    //    /// !only! numeric_col can be a measure
    //    ///  and therefore <numeric_col> should be only one across DataSet
    //    /// see how to validate dtype:
    //    /// https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    //    fn validate(&self) {}

    fn as_cacheable(&self) -> Option<&dyn CacheableDataSet> {
        Some(self)
    }
}

// TODO return Result
pub fn numeric_columns(schema: Arc<Schema>) -> Vec<String> {
    schema
        .iter_fields()
        .filter(|f| f.data_type().is_numeric())
        .map(|f| f.name.to_string())
        .collect::<Vec<String>>()
}

/// restrict columns which can be fields to Utf8 and Bool
pub fn fields_columns(schema: Arc<Schema>) -> Vec<String> {
    schema
        .iter_fields()
        .filter(|field| {
            matches!(
                field.data_type(),
                DataType::Utf8
                    | DataType::UInt8
                    | DataType::Int8
                    | DataType::UInt16
                    | DataType::Int16
                    | DataType::UInt32
                    | DataType::Int32
                    | DataType::UInt64
                    | DataType::Int64
            )
        })
        .map(|field| field.name.to_string())
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
        .map(|c| c.name.to_string())
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
            .map(|(x, m)| (x, m.aggregation()))
            .collect::<BTreeMap<&String, &Option<String>>>();

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
