use std::sync::Arc;

//use aws_sdk_s3::Region;
//use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;

//use std::borrow::Cow;
use futures::future::join_all;
use tokio::runtime::Builder;

use polars::prelude::{CsvReader, DataFrame, DataType, Field, Schema, SerReader};

pub fn multi_download(
    bucket: &str,
    keys: &[&str],
    cast_to_str: &[String],
    cast_to_f64: &[String],
) -> Vec<DataFrame> {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    let mut vc = Vec::with_capacity(cast_to_str.len() + cast_to_f64.len());
    for str_col in cast_to_str {
        vc.push(Field::new(str_col, DataType::Utf8))
    }
    for f64_col in cast_to_f64 {
        vc.push(Field::new(f64_col, DataType::Float64))
    }

    let schema = Schema::from_iter(vc);

    runtime.block_on(get_frames(bucket, keys, &schema))
}

async fn get_frames(bucket: &str, keys: &[&str], schema: &Schema) -> Vec<DataFrame> {
    //let region = "eu-west-2";
    //let region = Region::new(Cow::Borrowed(region));
    //let region_provider = RegionProviderChain::default_provider().or_else(region);
    let config = aws_config::from_env()
        //.region(region_provider)
        .load()
        .await;

    let client = Client::new(&config);

    let mut handles = Vec::with_capacity(keys.len());
    for key in keys.iter() {
        handles.push(get_frame(client.clone(), bucket, key, schema));
    }
    join_all(handles).await
}

async fn get_frame(client: Client, bucket: &str, key: &str, schema: &Schema) -> DataFrame {
    let req = client.get_object().bucket(bucket).key(key);
    let res = req.send().await.expect("Failed to send Get Object Request");

    let bytes = res
        .body
        .collect()
        .await
        .expect("Failed to collect aggregated bytes");
    let bytes = bytes.into_bytes();

    let cursor = std::io::Cursor::new(bytes);

    let df = CsvReader::new(cursor)
        .with_dtypes(Some(Arc::new(schema.clone())))
        .finish()
        .expect("Failed to read CSV");

    df
}
