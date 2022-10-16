import polars as pl
import pyarrow.csv as csv
import boto3

def s3_csv_filename_to_lst_srs (bucket: str, filename: str) -> list[pl.Series]:
    """function reads object via get_object(Bucket=bucket, Key=filename)

    Args:
        bucket (str): Bucket name
        filename (str): Key

    Returns:
        list[pl.Series]: _description_
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=filename)
    return s3_csv_obj_to_lst_srs(obj)

def s3_csv_obj_to_lst_srs (obj) -> list[pl.Series]:
    """converts s3 obj into polars frame calling pyarrow.csv.read_csv

    Args:
        obj (_type_): s3 object

    Returns:
        list[pl.Series]: _description_
    """
    table = csv.read_csv(obj['Body'])
    pf = pl.from_arrow(table)
    pf = pf.with_columns([pl.col("*").map(lambda s: s.cast(pl.Float64, strict=True) if s.is_numeric() else s)])
    return pf.get_columns() 