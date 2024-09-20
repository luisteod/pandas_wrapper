import io
import pandas as pd
import os
from tqdm import tqdm

class S3Pandas:
    """
    Class to handle pandas dataframes in S3.
    """
    def __init__(self, s3_client):
        self.s3_client = s3_client
    
    def read_folder(self, bucket: str, prefix: str) -> pd.DataFrame:
        df = pd.DataFrame()
        files = self.s3_client.list_files(bucket, prefix)
        for file in tqdm(files, desc="Reading files",leave=False):
            if file.endswith(".parquet"):
                data = self.s3_client.download_file(bucket, file)
                df = pd.concat([df, pd.read_parquet(io.BytesIO(data))])
        return df
    
    
    def save_dataframe(self, bucket: str, prefix: str, filename:str, df: pd.DataFrame) -> None:
        pq_bin = df.to_parquet(index=False, engine='pyarrow', compression="snappy")
        path = os.path.join(prefix, f"{filename}.snappy.parquet")
        self.s3_client.upload_file(pq_bin, bucket, path)
        part_num += 1

    def save_dataframe_in_chunks(self, bucket: str, prefix: str, df: pd.DataFrame, chunks: int) -> None:
        part_num = 0
        for i in tqdm(range(0, len(df), chunks), leave=False, desc="Uploading files"):
            start = i 
            if i + chunks >= len(df):
                end = len(df)
            else:
                end = i + chunks
            pq_bin = df[start:end].to_parquet(index=False, engine='pyarrow', compression="snappy")
            path = os.path.join(prefix, f"part-{part_num:05}.snappy.parquet")
            self.s3_client.upload_file(pq_bin, bucket, path)
            part_num += 1