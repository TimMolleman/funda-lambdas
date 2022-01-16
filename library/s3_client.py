import boto3
from botocore import client
import io
import logging
import os
import pandas as pd
from typing import List, Union, Optional


class S3Client:
    """Client for interacting with s3."""
    def __init__(self):
        self.s3: client.BaseClient = boto3.client('s3',
                                                  region_name='eu-west-2',
                                                  aws_access_key_id=os.environ['AMAZON_ACCESS_KEY_ID'],
                                                  aws_secret_access_key=os.environ['AMAZON_SECRET_ACCESS_KEY'])
        self.logger = self._init_logger()

    def list_objects(self, bucket: str, prefix: str):
        """Get objets info for a certain prefix/folder."""
        objects_ = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return objects_

    def read_object_data_to_df(self, bucket: str, key: str, parse_dates: Optional[List[str]] = None,
                               raise_on_missing: bool = True) -> pd.DataFrame:
        """For a certain bucket and key combination. Reads data from csv into pandas dataframe."""
        if not parse_dates:
            parse_dates = []

        # Get the object as a pandas dataframe
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj['Body'], parse_dates=parse_dates)
        except self.s3.exceptions.NoSuchKey as e:
            if raise_on_missing:
                self.logger.exception(f'Missing {bucket}/{key}.csv in s3')
                raise e
            df = pd.DataFrame({'link': [], 'time_added': []})

        return df

    def put_bytes_data_to_s3(self, bucket: str, key: str, bytes_object: Union[bytes, io.StringIO]) -> None:
        """Puts bytes data into s3 file."""
        if type(bytes_object) == bytes:
            self.s3.put_object(Bucket=bucket, Body=bytes_object, Key=key)
        elif type(bytes_object) == io.StringIO:
            self.s3.put_object(Bucket=bucket, Body=bytes_object.getvalue(), Key=key)

    @staticmethod
    def _init_logger():
        """Initialise logger for S3Client."""
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        return logger



