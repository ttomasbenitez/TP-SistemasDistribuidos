FROM python:3.11-slim
COPY gateway /gateway
COPY packages /packages

# Agregar /packages al PYTHONPATH
ENV PYTHONPATH="/packages:${PYTHONPATH}"

ENTRYPOINT ["python3", "/gateway/main.py"]
