import logging
import pandas as pd

from link_cleaner.src import constants
from library.s3_client import S3Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event={}, context={}):
    logger.info('Start processing of links')
    s3_client = S3Client()

    # Get historic and recent link dfs
    historic_links_df, most_recent_links_df, last_added_bucket_link, modified_time = \
        s3_client.get_historic_and_recent_links(constants.FUNDA_BUCKET)

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

    s3_client.write_updated_historic_and_recent_links(constants.FUNDA_BUCKET, most_recent_links_df, historic_links_df)


if __name__ == '__main__':
    main()
