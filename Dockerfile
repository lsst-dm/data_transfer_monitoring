FROM python:3.12-slim

ENV PYTHONUNBUFFERED=True

#ENV PYTHONASYINCIODEBUG=1

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY listeners .

COPY models .

COPY shared .

COPY main.py .

CMD [ "python", "main.py"]