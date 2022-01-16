import sqlalchemy
import pandas as pd
import os


class FundaDB:
    """Connector to the funda database for selecting house info."""
    def __init__(self):
        user = os.environ['FUNDA_DB_USER']
        password = os.environ['FUNDA_DB_PW']
        host = os.environ['FUNDA_DB_HOST']
        db = os.environ['FUNDA_DB_NAME']

        self.engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{host}/{db}')

    def query_house_data(self):
        """Query all housing info."""
        sql = sqlalchemy.text('SELECT * FROM city_info')
        df = pd.read_sql(sql, self.engine)
        return df
