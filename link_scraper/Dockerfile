FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY link_scraper ${LAMBDA_TASK_ROOT}
COPY library ${LAMBDA_TASK_ROOT}

# Install dependencies
COPY ./link_scraper/requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["link_scraper.launcher.main"]