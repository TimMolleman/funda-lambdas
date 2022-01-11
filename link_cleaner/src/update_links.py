import boto3
from botocore import client
import datetime as dt
import io
import logging
import os
import pandas as pd
from typing import Tuple

from link_cleaner.src import constants
import library

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event={}, context={}):
    logger.info('Start processing of links')
    s3 = boto3.client('s3',
                      region_name='eu-west-2',
                      aws_access_key_id=os.environ['AMAZON_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AMAZON_SECRET_ACCESS_KEY'])

    # Get historic and recent link dfs
    historic_links_df, most_recent_links_df, last_added_bucket_link, modified_time = \
        _get_historic_and_recent_links(s3, constants.FUNDA_BUCKET)

    # Get links
    historic_links = set(historic_links_df['link'])
    most_recent_links = set(most_recent_links_df['link'])

    # Get recent links but with historic links filtered out
    filtered_recent_links = most_recent_links - historic_links
    filtered_recent_links_df = pd.DataFrame({'link': list(filtered_recent_links),
                                             'time_added': modified_time})

    # Concat old historic links and the filtered recent links, and write away processed dfs
    historic_links_df = pd.concat([historic_links_df, filtered_recent_links_df])
    most_recent_links_df = (most_recent_links_df.loc[most_recent_links_df['link'].isin(filtered_recent_links)])

    # Add the modified time to the most_recent_links_df and sort the columns
    most_recent_links_df['time_added'] = modified_time
    most_recent_links_df = most_recent_links_df[constants.COLUMN_ORDER_MOST_RECENT]

    _write_updated_historic_and_recent_links(s3, constants.FUNDA_BUCKET, most_recent_links_df, historic_links_df)


def _get_historic_and_recent_links(s3: client.BaseClient, bucket: str) -> \
        Tuple[pd.DataFrame, pd.DataFrame, str, dt.datetime]:
    """Get historic and recent links from s3 as dataframes. Also get the last added bucket link and the
    time that this was added to the bucket."""
    # Get all historic links
    try:
        historic_links = s3.get_object(Bucket=bucket, Key=constants.HISTORIC_LINKS_KEY)
        historic_links_df = pd.read_csv(historic_links['Body'])

    # If historic links not available, initialise as empty dataframe
    except s3.exceptions.NoSuchKey:
        logger.warning('No historic-links.csv file found. Initialise empty dataframe instead')
        historic_links_df = pd.DataFrame({'link': [], 'time_added': []})

    # Get the most recently scraped housing links
    most_recent_links = s3.list_objects_v2(Bucket=bucket, Prefix=constants.PREFIX_MOST_RECENT_FOLDER)

    if most_recent_links.get('Contents'):
        if len(most_recent_links.get('Contents')) > 1:
            # Get the last modified filename
            get_last_modified = lambda link_obj: int(link_obj['LastModified'].strftime('%s'))
            most_recent_links = most_recent_links['Contents'][1:]
            last_added = [link_obj for link_obj in sorted(most_recent_links, key=get_last_modified, reverse=True)][0]

            # Get last_added file contents and also the time the file was inserted into s3
            last_added_bucket_link = last_added['Key']
            most_recent_links = s3.get_object(Bucket=bucket, Key=last_added_bucket_link)
            most_recent_links_df = pd.read_csv(most_recent_links['Body'])

            last_added_time = last_added['LastModified']
        else:
            message = f'No files found in the {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} folder'
            logger.error(message)
            raise Exception(message)

    else:
        message = f'The folder {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} does not exist'
        logger.error(message)
        raise Exception(message)

    return historic_links_df, most_recent_links_df, last_added_bucket_link, last_added_time


def _write_updated_historic_and_recent_links(s3: client.BaseClient, bucket: str,
                                             most_recent_links_df: pd.DataFrame,
                                             historic_links_df: pd.DataFrame) -> None:
    """Write the updated historic links and updated recent links away to s3."""
    logger.info(f'Writing most recent df: {len(most_recent_links_df)} rows. '
                f'And historic df: {len(historic_links_df)} rows')

    # Write away recent links
    csv_buffer = io.StringIO()
    most_recent_links_df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)
    s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(),
                  Key=f'{constants.PREFIX_FILTERED_MOST_RECENT}/filtered_most_recent.csv')

    # Write away historic links
    csv_buffer = io.StringIO()
    historic_links_df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)
    s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(), Key=constants.HISTORIC_LINKS_KEY)

    logger.info('Finished writing recent links and historic links')


if __name__ == '__main__':
    main()
