import boto3
import pandas as pd
from io import StringIO

def load_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")  # Automatically uses default profile or IAM role
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    return df
