FROM python:3.8

WORKDIR /app

COPY Scripts/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY start.sh start.sh
COPY . .

ENTRYPOINT ["./start.sh"]