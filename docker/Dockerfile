FROM python:3
WORKDIR /src

COPY . ./

RUN pip install jupyter
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install .

ADD https://raw.githubusercontent.com/vijayshinva/Indian-Stock-Markets/master/samples/01_Basic_Jupyter_Notebook/Indian_Stock_Markets.ipynb ./

EXPOSE 8888
ENTRYPOINT ["jupyter", "notebook", "Indian_Stock_Markets.ipynb"]
