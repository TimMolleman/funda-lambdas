FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY model_trainer ${LAMBDA_TASK_ROOT}/model_trainer
COPY library ${LAMBDA_TASK_ROOT}/library

# Install dependencies
COPY ./model_trainer/requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["model_trainer.src.model_trainer.main"]