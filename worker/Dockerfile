FROM python:3.11-slim
COPY worker /worker
COPY pkg /pkg
COPY Middleware /Middleware
COPY utils /utils

# Copiar el archivo de dependencias e instalarlo
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Agregar /pkg al PYTHONPATH
ENV PYTHONPATH="/worker:/pkg:/Middleware:/utils:${PYTHONPATH}"

ENTRYPOINT ["python3"]
