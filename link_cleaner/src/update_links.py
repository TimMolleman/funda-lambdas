import logging
import pandas as pd
from typing import Tuple

from library import constants
from library.exceptions import S3FileOrFolderNotFoundException
from library.s3_client import S3Client
from library import util

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event={}, context={}):
    logger.info('Start processing of links')
    s3_client = S3Client()

    # Get the historic links dataframe and most recent links info
    historic_links_df = s3_client.read_object_data_to_df(bucket=constants.FUNDA_BUCKET,
                                                         key=constants.HISTORIC_LINKS_KEY)
    most_recent_links = s3_client.list_objects(bucket=constants.FUNDA_BUCKET,
                                               prefix=constants.PREFIX_MOST_RECENT_FOLDER)

    # Get most_recent_links_df and recent modified_time
    most_recent_links_df, modified_time = _get_most_recent_links_df(most_recent_links, s3_client)

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

    # Create csv buffers
    most_recent_links_io = util.create_csv_buffer_object(most_recent_links_df)
    historic_links_io = util.create_csv_buffer_object(historic_links_df)

    # Write the buffers away
    s3_client.put_bytes_data_to_s3(bucket=constants.FUNDA_BUCKET,
                                   key=f'{constants.PREFIX_FILTERED_MOST_RECENT}/filtered_most_recent.csv',
                                   bytes_object=most_recent_links_io)
    s3_client.put_bytes_data_to_s3(bucket=constants.FUNDA_BUCKET,
                                   key=constants.HISTORIC_LINKS_KEY,
                                   bytes_object=historic_links_io)


def _get_most_recent_links_df(most_recent_links: dict, s3_client: S3Client) -> Tuple[pd.DataFrame, str]:
    """Gets the most recent links in a dataframe from the s3 result. Also gets the modification time of the last
    added file."""
    if most_recent_links.get('Contents'):
        # Check if there are contents else raise an exception
        if len(most_recent_links.get('Contents')) > 1:
            # Get the last modified filename
            get_last_modified = lambda link_obj: int(link_obj['LastModified'].strftime('%s'))
            most_recent_links = most_recent_links['Contents'][1:]
            last_added = \
                [link_obj for link_obj in sorted(most_recent_links, key=get_last_modified, reverse=True)][0]

            # Get last_added file contents and also the time the file was inserted into s3
            last_added_bucket_link = last_added['Key']
            most_recent_links_df = s3_client.read_object_data_to_df(bucket=constants.FUNDA_BUCKET,
                                                                    key=last_added_bucket_link)

            last_added_time = last_added['LastModified']
        else:
            message = f'No files found in the {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} folder'
            logger.error(message)
            raise S3FileOrFolderNotFoundException(message)

    else:
        message = f'The folder {constants.FUNDA_BUCKET}/{constants.PREFIX_MOST_RECENT_FOLDER} does not exist'
        logger.error(message)
        raise S3FileOrFolderNotFoundException(message)

    return most_recent_links_df, last_added_time


if __name__ == '__main__':
    main()
