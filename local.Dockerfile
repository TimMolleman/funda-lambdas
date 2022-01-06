FROM python:3.8-slim-buster

WORKDIR /app

# Copy function code
COPY . .

# Install dependencies
RUN pip3 install -r requirements.txt

CMD ["python", "launcher.py"]
