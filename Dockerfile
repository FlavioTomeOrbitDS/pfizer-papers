FROM python:3.10.10

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python", "covid-search_brasil-cardiologia.py"]
