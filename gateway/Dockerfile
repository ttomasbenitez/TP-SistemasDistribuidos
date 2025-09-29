FROM python:3.11-slim
COPY gateway /gateway
COPY packages /packages
COPY Middleware /Middleware

# Copiar el archivo de dependencias e instalarlo
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Agregar /packages al PYTHONPATH
ENV PYTHONPATH="/packages:${PYTHONPATH}"
ENV PYTHONPATH="/Middleware:${PYTHONPATH}"

ENTRYPOINT ["python3", "/gateway/main.py"]
