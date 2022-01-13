import boto3
from botocore import client
import datetime as dt
import io
import logging
import os
import pandas as pd
from typing import Tuple

from link_cleaner.src import constants


class S3Client:
    """Client for interacting with s3."""
    def __init__(self):
        self.s3: client.BaseClient = boto3.client('s3',
                                                  region_name='eu-west-2',
                                                  aws_access_key_id=os.environ['AMAZON_ACCESS_KEY_ID'],
                                                  aws_secret_access_key=os.environ['AMAZON_SECRET_ACCESS_KEY'])
        self.logger = self._init_logger()

    def get_historic_and_recent_links(self, bucket: str) -> \
            Tuple[pd.DataFrame, pd.DataFrame, str, dt.datetime]:
        """Get historic and recent links from s3 as dataframes. Also get the last added bucket link and the
        time that this was added to the bucket."""
        # Get all historic links
        try:
            historic_links = self.s3.get_object(Bucket=bucket, Key=constants.HISTORIC_LINKS_KEY)
            historic_links_df = pd.read_csv(historic_links['Body'])

        # If historic links not available, initialise as empty dataframe
        except self.s3.exceptions.NoSuchKey:
            self.logger.warning('No historic-links.csv file found. Initialise empty dataframe instead')
            historic_links_df = pd.DataFrame({'link': [], 'time_added': []})

        # Get the most recently scraped housing links
        most_recent_links = self.s3.list_objects_v2(Bucket=bucket, Prefix=constants.PREFIX_MOST_RECENT_FOLDER)

        if most_recent_links.get('Contents'):
            if len(most_recent_links.get('Contents')) > 1:
                # Get the last modified filename
                get_last_modified = lambda link_obj: int(link_obj['LastModified'].strftime('%s'))
                most_recent_links = most_recent_links['Contents'][1:]
                last_added = \
                    [link_obj for link_obj in sorted(most_recent_links, key=get_last_modified, reverse=True)][0]

                # Get last_added file contents and also the time the file was inserted into s3
                last_added_bucket_link = last_added['Key']
                most_recent_links = self.s3.get_object(Bucket=bucket, Key=last_added_bucket_link)
                most_recent_links_df = pd.read_csv(most_recent_links['Body'])

                last_added_time = last_added['LastModified']
            else:
                message = f'No files found in the {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} folder'
                self.logger.error(message)
                raise Exception(message)

        else:
            message = f'The folder {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} does not exist'
            self.logger.error(message)
            raise Exception(message)

        return historic_links_df, most_recent_links_df, last_added_bucket_link, last_added_time

    def write_updated_historic_and_recent_links(self, bucket: str, most_recent_links_df: pd.DataFrame,
                                                historic_links_df: pd.DataFrame) -> None:
        """Write the updated historic links and updated recent links away to s3."""
        self.logger.info(f'Writing most recent df: {len(most_recent_links_df)} rows. '
                         f'And historic df: {len(historic_links_df)} rows')

        # Write away recent links
        csv_buffer = io.StringIO()
        most_recent_links_df.to_csv(csv_buffer, header=True, index=False)
        csv_buffer.seek(0)
        self.s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(),
                           Key=f'{constants.PREFIX_FILTERED_MOST_RECENT}/filtered_most_recent.csv')

        # Write away historic links
        csv_buffer = io.StringIO()
        historic_links_df.to_csv(csv_buffer, header=True, index=False)
        csv_buffer.seek(0)
        self.s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(), Key=constants.HISTORIC_LINKS_KEY)

        self.logger.info('Finished writing recent links and historic links')

    def read_object_to_df(self, bucket: str, key: str):
        # Retrieve history links object from S3
        try:
            historic_links_obj = self.s3.get_object(Bucket=constants.FUNDA_BUCKET, Key=constants.HISTORIC_LINKS_KEY)
            history_links_df = pd.read_csv(historic_links_obj['Body'], parse_dates=['time_added'])
        except self.s3.exceptions.NoSuchKey as e:
            self.logger.exception('Missing history-links.csv in s3')
            raise e

    @staticmethod
    def _init_logger():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        return logger



