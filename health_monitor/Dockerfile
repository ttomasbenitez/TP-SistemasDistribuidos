FROM python:3.11-slim

COPY health_monitor /health_monitor
COPY utils /utils
COPY pkg /pkg
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH="/health_monitor:/utils:/pkg:${PYTHONPATH}"

ENTRYPOINT ["python3", "/health_monitor/main.py"]
