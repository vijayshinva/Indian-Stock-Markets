FROM python:3

COPY src/ ./indian_stock_markets
WORKDIR /indian_stock_markets

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install .

CMD [ "python"]
