pub mod datasource;
pub mod new;

use std::collections::{BTreeMap, HashSet};

use polars::prelude::*;
use serde::{ser::SerializeMap, Serialize, Serializer};

use crate::cache::{Cache, CacheableDataSet};
use crate::errors::{UltiResult, UltimaErr};
use crate::execute;
use crate::filters::AndOrFltrChain;
use crate::reports::report::ReportersMap;
use crate::{CalcParameter, ComputeRequest, MeasuresMap};
use once_cell::sync::Lazy;

use self::datasource::DataSource;
pub static EMPTY_REPORTS_MAP: Lazy<ReportersMap> = Lazy::new(Default::default);

/// This is the default struct which implements Dataset
/// Usually a client/user would overwrite it with their own DataSet
#[derive(Default)]
pub struct DataSetBase {
    /// Data
    pub source: DataSource,
    /// Stores measures map, ie what you want to calculate
    pub measures: MeasuresMap,
    ///Similar to measures, but stores Reports
    pub reports: ReportersMap,
    /// build_params can be used in .prepare() - if streaming or add_row
    ///  this happens during the execution. Hence we can't remove and pass to .prepare() directly
    pub config: BTreeMap<String, String>,
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

    /// Since we support a limited number of data sources, each [DataSet] must contain a source.
    /// Since many of the [DataSet] methods' logic depends on the variant of the source, we implement those there    
    fn get_datasource(&self) -> &DataSource;

    /// Get all Measures associated with the DataSet
    /// TODO by default coauld be numeric columns accessed via [get_lazyframe]
    fn get_measures(&self) -> &MeasuresMap;

    /// Clones but clone is cheap
    /// Polars DataFrame clone is cheap:
    /// https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    /// This method gets the main LazyFrame of the Dataset
    fn get_lazyframe(&self, filters: &AndOrFltrChain) -> LazyFrame {
        self.get_datasource().get_lazyframe(filters)
    }

    /// Get Schema (Column Names and DataTypes) of the underlying Data
    /// !Default implementation calls `.get_lazyframe(&vec![])`, so if `get_lazyframe` materialises/loads data (eg from DB via a connector)
    /// Be careful, this might break your app.
    fn get_schema(&self) -> UltiResult<Arc<Schema>> {
        self.get_datasource().get_schema()
    }

    /// !Default implementation assumes the DataSet is an InMemory DataSet, and has been prepared.
    /// Therefore by default we don't prepare on each compute request.
    /// * `streaming` - See polars streaming. Use when your LazyFrame is a Scan if you don't want to load whole frame
    /// into memory. See: https://www.rhosignal.com/posts/polars-dont-fear-streaming/
    fn compute(&self, r: ComputeRequest) -> UltiResult<DataFrame> {
        execute(self, r, self.get_datasource().prepare_on_each_request())
    }

    /// Get a column. Potentially this will be removed in favour of get_columns
    /// !Default implementation calls `.get_lazyframe(&vec![])`, so if `get_lazyframe` materialises/loads data (eg from DB via a connector)
    /// Be careful, this might break your app.
    fn get_column(&self, col_name: &str) -> UltiResult<Series> {
        self.get_lazyframe(&vec![])
            .select([col(col_name)])
            .collect()?
            .pop() //above select guaranteed one column
            .ok_or(UltimaErr::Other(format!("Column {col_name} doesn't exist")))
    }

    /// Get all Reporters associated with the DataSet
    fn get_reporters(&self) -> &ReportersMap {
        &EMPTY_REPORTS_MAP
    }

    /// Modify lf in place - applicable only to InMemory DataSet
    /// Common use case - prepare, and then set_inplace
    fn set_lazyframe_inplace(&mut self, _: LazyFrame) -> UltiResult<()> {
        Err(UltimaErr::Other(
            "set_lazyframe_inplace is Not implemented for your Data Set".to_string(),
        ))
    }

    /// Collects the (main) LazyFrame of the DataSet
    /// Will return an error if [DataSet::set_lazyframe_inplace] is not implemented
    fn collect(&mut self) -> UltiResult<()> {
        let lf = self.get_lazyframe(&vec![]).collect()?.lazy();
        self.set_lazyframe_inplace(lf).map_err(|err| {
            UltimaErr::Other(format!(
                "Error calling .collect(), followed by
            an attempt to set Data inplace: {err}. Does it make sence to collect you Datasource?",
            ))
        })?;
        Ok(())
    }

    /// Prepare runs BEFORE any calculations. In eager mode it runs ONCE
    /// Any pre-computations which are common to all queries could go in here.
    /// Calls [DataSet::prepare_frame]
    /// Will return an error if [DataSet::set_lazyframe_inplace] is not implemented
    fn prepare(&mut self) -> UltiResult<()>
//where
    //    Self: Sized,
    {
        let new_frame = self.prepare_frame(self.get_lazyframe(&vec![]))?;
        self.set_lazyframe_inplace(new_frame).map_err(|err| {
            UltimaErr::Other(format!(
                "Error calling .prepare(), followed by
            an attempt to set Data inplace: {err}. Does it make sence to prepare you Datasource? Has your DataSet already been prepared?"
            ))
        })?;
        Ok(())
    }

    /// By returning a Frame this method can be used on a
    /// *lf - buffer. if None, function "prepares" self.lazy_frame()
    fn prepare_frame(&self, lf: LazyFrame) -> UltiResult<LazyFrame> {
        Ok(lf)
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
    /// Good usecase: add prepared
    fn overridable_columns(&self) -> Vec<String> {
        self.get_schema()
            .map(overridable_columns)
            .unwrap_or_default()
    }
    /// Validate DataSet
    /// Runs once, making sure all the required columns, their contents, types etc are valid
    /// Should contain an optional flag for analysis(ie displaying statistics of filtered out items, saving those as CSVs)
    /// *_validation_set - at different points in the runtime it makes sence to validate different subsets of the data
    /// eg FRTB validate before or after .prepare(). User can control that through _validation_set param
    fn validate_frame(&self, _: Option<&LazyFrame>, _validation_set: u8) -> UltiResult<()> {
        Ok(())
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
    fn get_datasource(&self) -> &DataSource {
        &self.source
    }

    /// Modify lf in place - applicable only to InMemory DataSource
    fn set_lazyframe_inplace(&mut self, lf: LazyFrame) -> UltiResult<()> {
        if let DataSource::InMemory(_) = self.source {
            self.source = DataSource::InMemory(lf.collect()?)
        } else {
            return Err(UltimaErr::Other("Can't set data inplace with this Source. Currently can only set In Memory Dataframe".to_string()));
        }
        Ok(())
    }

    fn get_measures(&self) -> &MeasuresMap {
        &self.measures
    }

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
pub fn overridable_columns(schema: Arc<Schema>) -> Vec<String> {
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
        // For measures we are only interested in their agg method
        let measures = self
            .get_measures()
            .iter()
            .map(|(x, m)| (x, m.aggregation()))
            .collect::<BTreeMap<&String, &Option<String>>>();
        let ordered_measures: BTreeMap<_, _> = measures.iter().collect();

        let utf8_cols = self.get_schema().map(fields_columns).unwrap_or_default();
        let calc_params = self.calc_params();

        let mut seq = serializer.serialize_map(Some(4))?;

        seq.serialize_entry("fields", &utf8_cols)?;
        seq.serialize_entry("measures", &ordered_measures)?;
        seq.serialize_entry("calc_params", &calc_params)?;
        seq.end()
    }
}
