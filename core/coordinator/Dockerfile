FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY ./core/coordinator/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./core/coordinator /app/

COPY ./core/common/schema.py /app/schema.py

COPY ./core/plugins /app/plugins

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

