use crate::{
    datasource::DataSource,
    derive_basic_measures_vec, numeric_columns,
    reports::report::{Reporter, ReportersMap},
    DataSet, DataSetBase, DataSourceConfig, Measure, MeasuresMap, CPM,
};

pub trait NewSourcedDataSet: DataSet {
    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn new(source: DataSource, mm: MeasuresMap, rm: ReportersMap, params: CPM) -> Self
    where
        Self: Sized;

    /// *rm - Reports
    /// *ms - Measures
    /// Either place your desired numeric columns and bespokes in
    /// *ms and set include_numeric_cols_as_measures = False
    /// or set your bespokes in *ms and include_numeric_cols_as_measures = True
    /// See [DataSetBase] and [CalcParameter] for description of the parameters
    fn from_vec<M, R>(
        source: DataSource,
        mm: M,
        include_numeric_cols_as_measures: bool,
        rm: R,
        params: CPM,
    ) -> Self
    where
        Self: Sized,
        M: IntoIterator<Item = Measure>,
        R: IntoIterator<Item = Reporter>,
    {
        let mut ms = Vec::from_iter(mm);
        if include_numeric_cols_as_measures {
            let num_cols = source
                .get_schema()
                .map(numeric_columns)
                .expect("Failed to obtain numeric columns");

            let numeric_cols_as_measures = derive_basic_measures_vec(num_cols);
            ms.extend(numeric_cols_as_measures);
        }

        let mm: MeasuresMap = MeasuresMap::from_iter(ms);
        let rm: ReportersMap = ReportersMap::from_iter(rm);
        Self::new(source, mm, rm, params)
    }

    fn from_config(conf: DataSourceConfig) -> Self
    where
        Self: Sized,
    {
        let (frame, measure_cols, bp) = conf.build();
        let mm: MeasuresMap = MeasuresMap::from_iter(measure_cols);
        Self::new(frame, mm, Default::default(), bp)
    }
}

impl NewSourcedDataSet for DataSetBase {
    fn new(source: DataSource, mm: MeasuresMap, _: ReportersMap, config: CPM) -> Self {
        Self {
            source,
            measures: mm,
            config,
            ..Default::default()
        }
    }
}
