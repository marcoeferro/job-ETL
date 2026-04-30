# Dockerfile (en la raíz del proyecto)
FROM python:3.11-slim

WORKDIR /app

# Copiar requirements y src
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Instalar src como paquete editable
RUN pip install -e .

# Variable para que Python encuentre el src/
ENV PYTHONPATH=/app/src:$PYTHONPATH

CMD ["python", "-m", "main"]