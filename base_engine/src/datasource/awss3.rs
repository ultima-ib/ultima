//use aws_sdk_s3::Region;
//use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;

//use std::borrow::Cow;
use tokio::runtime::Builder;
use futures::future::join_all;

use polars::prelude::{DataFrame, CsvReader, SerReader};

pub fn multi_download(bucket: &str, keys: &[&str], cast_to_str: &[String], cast_to_f64: &[String]) -> Vec<DataFrame> {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let bucket = "ultima-bi";
    let keys = ["Delta.csv", "testset.csv"];
    
    runtime.block_on(get_frames(bucket, &keys))
    
}

async fn get_frames(bucket: &str, keys: &[&str]) -> Vec<DataFrame> {
    //let region = "eu-west-2";
    //let region = Region::new(Cow::Borrowed(region));
    //let region_provider = RegionProviderChain::default_provider().or_else(region);
    let config = aws_config::from_env()
    //.region(region_provider)
    .load().await;

    let client = Client::new(&config);

    

    let mut handles = Vec::with_capacity(keys.len());
    for key in keys.into_iter() {
        handles.push(get_frame(client.clone(), bucket, key));
    }
    join_all(handles).await

}

async fn get_frame(client: Client, bucket: &str, key: &str) -> DataFrame {
    let req = client.get_object().bucket(bucket).key(key);
    let res = req.send().await.expect("Failed to send Get Object Request");

    let bytes = res.body.collect().await.expect("Failed to collect aggregated bytes");
    let bytes = bytes.into_bytes();

    let cursor = std::io::Cursor::new(bytes);

    let df = CsvReader::new(cursor).finish().expect("Failed to read CSV");

    df
}