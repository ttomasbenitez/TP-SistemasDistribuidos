FROM python:3.11-slim
COPY worker /worker
COPY pkg /pkg
COPY Middleware /Middleware

# Copiar el archivo de dependencias e instalarlo
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Agregar /pkg al PYTHONPATH
ENV PYTHONPATH="/pkg:${PYTHONPATH}"
ENV PYTHONPATH="/Middleware:${PYTHONPATH}"

ENTRYPOINT ["python3"]
