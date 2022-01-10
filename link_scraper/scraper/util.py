import os
import datetime as dt


def is_in_aws() -> bool:
    # Check if the execution environment is AWS
    return os.getenv('AWS_EXECUTION_ENV') is not None


def get_feed_uri() -> str:
    dt_string = (dt.datetime
                 .now()
                 .strftime('%Y-%m-%dT%H:%M:%S'))

    if is_in_aws():
        # AWS Lambda can only write to the /tmp folder
        feed_uri = "s3://funda-airflow/house-links/house-links-recent/funda-spider-{}.csv".format(dt_string)

    else:
        feed_uri = "file://{}/funda-spider-{}.csv".format(os.path.join(os.getcwd(), "feed"), dt_string)

    return feed_uri


def extract_digits_from_str(str_: str, square_meters: bool = False) -> int:
    stripped_str = ''.join(c for c in str_ if c.isdigit())
    if square_meters:
        stripped_str = int(stripped_str[:-1])
    else:
        stripped_str = int(stripped_str)

    return stripped_str
