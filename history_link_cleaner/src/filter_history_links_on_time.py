import logging
import datetime as dt
import io
import pytz

from library import constants
from library.s3_client import S3Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(event={}, context={}):
    logger.info('Start filtering of historic links document')

    # Retrieve history links object from s3
    s3_client = S3Client()
    history_links_df = s3_client.read_object_data_to_df(constants.FUNDA_BUCKET, constants.HISTORIC_LINKS_KEY,
                                                        parse_dates=['time_added'])

    # Filter out links in history-links.csv that are passed a certain retention time
    current_dt = dt.datetime.now(tz=pytz.utc)
    retention_time = dt.timedelta(days=30)
    cutoff_dt = (current_dt - retention_time)
    history_links_df = history_links_df.loc[history_links_df['time_added'] > cutoff_dt]

    # Write away cleaned historic links
    csv_buffer = io.StringIO()
    history_links_df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)

    # Put the bytes data to
    s3_client.put_bytes_data_to_s3(bucket=constants.FUNDA_BUCKET,
                                   key=constants.HISTORIC_LINKS_KEY,
                                   bytes_object=csv_buffer)

    logger.info('Finished filtering of historic links document')


if __name__ == '__main__':
    main()
