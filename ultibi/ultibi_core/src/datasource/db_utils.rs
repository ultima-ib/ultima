use arrow_array::RecordBatch;
use polars::{
    functions::concat_df_diagonal,
    prelude::{DataFrame, Field},
    series::Series,
};
use polars_arrow::array::Array;

use crate::errors::{UltiResult, UltimaErr};

pub fn record_batches_to_df<I>(batches: I) -> UltiResult<DataFrame>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let batches_iter: <I as IntoIterator>::IntoIter = batches.into_iter();

    let mut dfs = vec![];

    for next_batch in batches_iter {
        dfs.push(batch_to_df(next_batch));
    }

    concat_df_diagonal(&dfs).map_err(UltimaErr::Polars)
}

pub fn batch_to_df(batch: RecordBatch) -> DataFrame {
    let mut columns = vec![];
    batch
        .schema()
        .all_fields()
        .into_iter()
        .zip(batch.columns())
        .for_each(|(f, c)| {
            let polars_arrow_field = polars_arrow::datatypes::Field::from(f);
            let polars_arrow_datatype = polars_arrow_field.data_type;
            let polars_arrow_name = polars_arrow_field.name;
            let polars_field = Field::new(&polars_arrow_name, (&polars_arrow_datatype).into());
            // let polars_field = Field::from(f);
            let chunk: Box<dyn Array> = From::from(c.as_ref());
            let s = unsafe {
                Series::from_chunks_and_dtype_unchecked(
                    polars_field.name.as_str(),
                    vec![chunk],
                    polars_field.data_type(),
                )
            };
            columns.push(s);
        });
    DataFrame::from_iter(columns)
}
