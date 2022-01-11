FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY history_link_cleaner ${LAMBDA_TASK_ROOT}/history_link_cleaner
COPY library ${LAMBDA_TASK_ROOT}/library

# Install dependencies
COPY ./history_link_cleaner/requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["history_link_cleaner.src.filter_history_links_on_time.main"]