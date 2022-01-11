import boto3
import pandas as pd
import logging
import datetime as dt
import io
import os
import pytz

from history_link_cleaner.src import constants
import library

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event={}, context={}):
    logger.info('Start filtering of historic links document')
    s3 = boto3.client('s3',
                      region_name='eu-west-2',
                      aws_access_key_id=os.environ['AMAZON_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AMAZON_SECRET_ACCESS_KEY'])

    # Retrieve history links object from S3
    try:
        historic_links_obj = s3.get_object(Bucket=constants.FUNDA_BUCKET, Key=constants.HISTORIC_LINKS_KEY)
        history_links_df = pd.read_csv(historic_links_obj['Body'], parse_dates=['time_added'])
    except s3.exceptions.NoSuchKey as e:
        logger.exception('Missing history-links.csv in s3')
        raise e

    # Filter out links in history-links.csv that are passed a certain retention time
    current_dt = dt.datetime.now(tz=pytz.utc)
    retention_time = dt.timedelta(days=30)
    cutoff_dt = (current_dt - retention_time)
    history_links_df = history_links_df.loc[history_links_df['time_added'] > cutoff_dt]

    # Write away cleaned historic links
    csv_buffer = io.StringIO()
    history_links_df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)
    s3.put_object(Bucket=constants.FUNDA_BUCKET, Body=csv_buffer.getvalue(), Key=constants.HISTORIC_LINKS_KEY)

    logger.info('Finished filtering of historic links document')


if __name__ == '__main__':
    main()
