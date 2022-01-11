import boto3
import pickle
import datetime as dt
import os


class S3Client:
    def __init__(self):
        self.s3 = boto3.client('s3',
                               region_name='eu-west-2',
                               aws_access_key_id=os.environ['AMAZON_ACCESS_KEY_ID'],
                               aws_secret_access_key=os.environ['AMAZON_SECRET_ACCESS_KEY'])

    def write_model_to_s3_file(self, model) -> None:
        pickle_byte_obj = pickle.dumps(model)
        self.s3.put_object(Bucket='funda-airflow', Key=f'models/fundamodel_{dt.date.today()}.sav',
                           Body=pickle_byte_obj)
