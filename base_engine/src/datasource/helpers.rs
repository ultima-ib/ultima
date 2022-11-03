use std::collections::HashMap;

use polars::{
    prelude::{
        col, DataFrame, DataType, Expr, Field, IntoLazy, JoinType, LazyCsvReader, NamedFrom, Schema,
    },
    series::Series,
};

use crate::{derive_basic_measures_vec, numeric_columns, Measure};

/// creates an empty frame with columns
pub fn empty_frame(with_columns: &[String]) -> DataFrame {
    let mut x: Vec<Series> = Vec::with_capacity(with_columns.len());
    let y: [String; 0] = [];
    for c in with_columns {
        x.push(Series::new(c, &y));
    }
    DataFrame::new_no_checks(x)
}

/// reads DataFrame from path, casts cols to str and numeric cols to f64
pub fn path_to_df(path: &str, cast_to_str: &[String], cast_to_f64: &[String]) -> DataFrame {
    let mut vc = Vec::with_capacity(cast_to_str.len() + cast_to_f64.len());
    for str_col in cast_to_str {
        vc.push(Field::new(str_col, DataType::Utf8))
    }
    for f64_col in cast_to_f64 {
        vc.push(Field::new(f64_col, DataType::Float64))
    }

    let schema = Schema::from_iter(vc);

    // if path provided, then we expect it to be of the correct format
    // unrecoverable. Panic if failed to read file
    let df = LazyCsvReader::new(path)
        .has_header(true)
        .with_parse_dates(true)
        .with_dtype_overwrite(Some(&schema))
        //.with_ignore_parser_errors(ignore)
        .finish()
        .and_then(|lf| lf.collect())
        .unwrap_or_else(|_| panic!("Error reading file: {path}"));

    df
}

pub fn finish(
    a2h: Vec<String>,
    f2a: Vec<String>,
    measures: Vec<String>,
    mut df_attr: DataFrame,
    df_hms: DataFrame,
    mut concatinated_frame: DataFrame,
    build_params: HashMap<String, String>,
) -> (DataFrame, Vec<Measure>, HashMap<String, String>) {
    // join with hms if a2h was provided
    if !a2h.is_empty() {
        let a2h_expr = a2h.iter().map(|c| col(c)).collect::<Vec<Expr>>();
        df_attr = df_attr.lazy()
            .join(df_hms.lazy(), a2h_expr.clone(), a2h_expr, JoinType::Left)
            .collect()
            .expect("Could not join attributes to hms. Review attributes_join_hierarchy field in the setup");
    }
    // if files to attributes was provided
    if !f2a.is_empty() {
        let f2a_expr = f2a.iter().map(|c| col(c)).collect::<Vec<Expr>>();
        concatinated_frame = concatinated_frame.lazy()
            .join(df_attr.lazy(), f2a_expr.clone(), f2a_expr, JoinType::Outer)
            .collect()
            .expect("Could not join files with attributes-hms. Review files_join_attributes field in the setup");
    }

    // if measures were provided
    let measures = if !measures.is_empty() {
        // Checking if each measure is present in DF
        measures.iter().for_each(|col| {
            concatinated_frame
                .column(col)
                .unwrap_or_else(|_| panic!("Column {} not found", col));
        });
        derive_basic_measures_vec(measures)
    }
    // If not provided return all numeric columns
    else {
        let num_cols = numeric_columns(&concatinated_frame);
        derive_basic_measures_vec(num_cols)
    };

    (concatinated_frame, measures, build_params)
}
