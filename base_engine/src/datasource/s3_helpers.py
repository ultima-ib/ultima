import polars as pl
import pyarrow.csv as csv
import boto3

def s3_csv_to_lst_srs (bucket, filename):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=filename)
    table = csv.read_csv(obj['Body'])
    pf = pl.from_arrow(table)
    pf = pf.with_columns([pl.col("*").map(lambda s: s.cast(pl.Float64, strict=True) if s.is_numeric() else s)])
    return pf.get_columns() 