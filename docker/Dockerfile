FROM python:3

COPY src/ ./
RUN pip install --no-cache-dir -r indian_stock_markets/requirements.txt
RUN pip install indian_stock_markets/.

CMD [ "python"]
