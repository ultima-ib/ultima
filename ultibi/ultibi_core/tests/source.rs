use std::path::PathBuf;

use polars::prelude::{LazyCsvReader, LazyFileListReader};
use ultibi_core::{DataSet, Source, DataSetBase, new::NewSourcedDataSet};
mod common;

#[test]
#[should_panic(expected = "Can't set data inplace with this Source")]
fn prepare_scanned() {

    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "testset.csv"]
        .iter()
        .collect();

    let df = LazyCsvReader::new(path).finish().unwrap();

    let mut data: DataSetBase = DataSetBase::from_vec(Source::Scan(df), vec![], true, vec![], Default::default());

    data.prepare().unwrap();
}

#[test]
#[should_panic(expected = "Can't set data inplace with this Source")]
fn collect_scanned() {

    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "testset.csv"]
        .iter()
        .collect();

    let df = LazyCsvReader::new(path).finish().unwrap();

    let mut data: DataSetBase = DataSetBase::from_vec(Source::Scan(df), vec![], true, vec![], Default::default());

    data.collect().unwrap();


}