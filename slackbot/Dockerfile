FROM python:3.8

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

CMD ["python", "post_tweet.py"]