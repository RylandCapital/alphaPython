FROM python:3.8

WORKDIR /app

COPY Scripts/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "Scripts/alphadefi.fund.py"]