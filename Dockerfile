FROM python:3.8

WORKDIR /app

COPY Scripts/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
COPY /etc/secrets/.env .env

CMD ["python", "Scripts/collectData.py"]