FROM python:3

ADD crypto_to_influxdb.py /
ADD requirements.txt /

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

CMD [ "python3", "./crypto_to_influxdb.py" ]