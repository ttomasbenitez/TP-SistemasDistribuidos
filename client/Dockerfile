FROM python:3.11-slim
COPY client /client
COPY pkg /pkg

# Agregar /pkg al PYTHONPATH
ENV PYTHONPATH="/pkg:${PYTHONPATH}"

ENTRYPOINT ["python3", "/client/main.py"]
