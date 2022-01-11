FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY link_cleaner ${LAMBDA_TASK_ROOT}/link_cleaner
COPY library ${LAMBDA_TASK_ROOT}/library

# Install dependencies
COPY ./link_cleaner/requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["link_cleaner.src.update_links.main"]