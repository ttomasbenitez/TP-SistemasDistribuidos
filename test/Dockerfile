FROM python:3.9-slim-buster

WORKDIR /app

COPY test/requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["pytest", "-q"]