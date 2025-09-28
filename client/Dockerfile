FROM python:3.11-slim
COPY client /client
COPY packages /packages

# Agregar /packages al PYTHONPATH
ENV PYTHONPATH="/packages:${PYTHONPATH}"

ENTRYPOINT ["python3", "/client/main.py"]
