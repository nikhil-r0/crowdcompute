FROM python:3.11-slim

WORKDIR /app

COPY ./core/worker/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./core/worker /app/

COPY ./core/common/schema.py /app/schema.py

COPY ./core/plugins /app/plugins

EXPOSE 8000

CMD ["python", "main.py"]