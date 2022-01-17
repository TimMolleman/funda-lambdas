# Funda Housing Price Prediction Project (Lambdas)

Practice project for setting up a data infrastructure for gathering data, doing some operations on it,
training a model, and then making it available via an API. Primary tooling used for this are Apache Airflow for orchestration of
several AWS services (mainly Lambdas), AWS Lambda in combination with Python and FastAPI for creating the API endpoints.
This FastAPI service is being hosted via AWS API Gateway and Lambda.

For the project, basic housing information of the Dutch real-estate website [Funda](https://www.funda.nl/) is scraped and saved
to an Amazon S3 bucket. After this some transformations are done, and a model is trained, all using AWS Lambda functions
[(see repository)](https://github.com/TimMolleman/funda-link-scraper). The trained  model is also saved to S3 and is then exposed for predictions via the 
API [(see repository)](https://github.com/TimMolleman/funda-api). To schedule all lambdas and to do a number of other
transformations Apache Airflow is used [(see repository)](https://github.com/TimMolleman/funda-airflow).

## Description
This repository contains the code for the lambda functions that are invoked via Apache Airflow.
To be exact, it contains four modules of code with accessory Dockerfiles and requirements.txt for creating the lambda image:

* link_scraper: For scraping and storing the information from Funda.
* link_cleaner: For cleaning up links. Checks if links not in historic data to avoid duplicates and updates historic S3 file to keep accurate track of links.
* history_link_cleaner: Periodically dropping links from file that are older than N days.
* model_trainer: Lambda for training model on house data. Is a simple regression model.

## Getting Started

### Dependencies
The Python version recommended for running this project is 3.8.
It is possible to test the scripts locally. Every Lambda has its own requirements.txt, so it is recommended
to create a virtual environment to install the packages into for the Lambda you want to test.

However, it is also possible to build the Docker image and locally test the invoking of the Lambda function
as it would on AWS.

### Executing program
To run the Lambda scripts locally it is possible to simply run the .py files like:
```
python3 update_links.py
```

For testing the scripts as actual Lambda invocations, information can be [found here](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html).

## Authors
Tim Molleman