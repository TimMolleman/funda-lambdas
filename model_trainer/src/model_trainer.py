from funda_db import FundaDB
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import KFold, cross_val_score
import numpy as np
import logging

from s3_client import S3Client
import library

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(events={}, context={}):
    # Get the housing data for training the model
    funda_db = FundaDB()
    df = funda_db.query_house_data()
    df_x = df.drop(columns=['link', 'time_added', 'city', 'price'])
    df_y = df['price']
    logger.info('Start model training')

    # Do a K-fold cross-validation to see accuracy of model
    cv = KFold(n_splits=5, shuffle=True)

    # Create the model
    model = LinearRegression()

    # Get cross_validation_score
    scores = cross_val_score(model, df_x, df_y, scoring='explained_variance', cv=cv, n_jobs=-1)
    logger.info(f'Explained variance: {np.mean(scores)} ({np.std(scores)})')

    # Fit the eventual model for use on all data
    model.fit(df_x, df_y)

    # Write model away
    s3_client = S3Client()
    s3_client.write_model_to_s3_file(model)


if __name__ == '__main__':
    main()
