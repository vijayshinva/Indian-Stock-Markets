FROM python:3

COPY indian-stock-markets/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY indian-stock-markets/ .

CMD [ "python"]
