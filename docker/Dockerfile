FROM python:3

COPY indian_stock_markets/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY indian_stock_markets/ .

CMD [ "python"]
