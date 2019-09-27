import requests
import zipfile
import shutil
import csv
import pandas as pd
from datetime import date
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


class BhavCopy(object):
    """description of class"""

    def __init__(self, date: date):
        self.date = date
        self._url_eq = urlparse(
            f'https://www.nseindia.com/content/historical/EQUITIES/{date.strftime("%Y")}/{date.strftime("%b").upper()}/cm{date.strftime("%d%b%Y").upper()}bhav.csv.zip')
        self._file_eq = Path(self._url_eq.path[1:-4])
        self._url_fo = urlparse(
            f'https://www.nseindia.com/content/historical/DERIVATIVES/{date.strftime("%Y")}/{date.strftime("%b").upper()}/fo{date.strftime("%d%b%Y").upper()}bhav.csv.zip')
        self._file_fo_zip = Path(self._url_fo.path[1:])
        self._file_fo = Path(self._url_fo.path[1:-4])
        self._url_short_selling = urlparse(
            f'https://www.nseindia.com/archives/equities/shortSelling/shortselling_{date.strftime("%d%m%Y")}.csv')
        self._file_short_selling = Path(self._url_short_selling.path[1:])
        self._url_participant_oi = urlparse(
            f'https://www.nseindia.com/content/nsccl/fao_participant_oi_{date.strftime("%d%m%Y")}.csv')
        self._file_participant_oi = Path(self._url_participant_oi.path[1:])
        self.market_close = False
        self._initialize()

    def _initialize(self):
        if self.date.weekday() == 5 or self.date.weekday() == 6:
            self.market_close = True
            return
        self._try_download(self._url_eq)
        self._try_download(self._url_fo)
        self._try_download(self._url_short_selling)
        self._try_download(self._url_participant_oi)

    def _try_download(self, url: urlparse):
        path = Path(url.path[1:])
        if not path.is_file():
            path.parent.mkdir(parents=True, exist_ok=True)
            with requests.get(url.geturl(), stream=True) as r:
                if r.status_code == 200:
                    with path.open('wb') as f:
                        r.raw.decode_content = True
                        shutil.copyfileobj(r.raw, f)
                    if path.suffix == '.zip':
                        with zipfile.ZipFile(path, 'r') as zf:
                            zf.extractall(path.parent)
                if r.status_code == 404:
                    self.market_close = True

    def read_fo(self):
        if self._file_fo.is_file():
            with self._file_fo.open('rt') as f:
                csv_reader = csv.reader(f, delimiter=',')
                self.headers_fo = next(csv_reader, None)
                for row in csv_reader:
                    yield (row[0], row[1], datetime.strptime(row[2], '%d-%b-%Y').strftime('%Y-%m-%d'), row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], datetime.strptime(row[14], '%d-%b-%Y').strftime('%Y-%m-%d'))

    def read_eq(self):
        if self._file_eq.is_file():
            with self._file_eq.open('rt') as f:
                csv_reader = csv.reader(f, delimiter=',')
                self.headers_eq = next(csv_reader, None)
                for row in csv.reader(f, delimiter=','):
                    yield row

    def read_short_selling(self):
        if self._file_short_selling.is_file():
            with self._file_short_selling.open('rt') as f:
                csv_reader = csv.reader(f, delimiter=',')
                self.headers_short_selling = next(csv_reader, None)
                for row in csv.reader(f, delimiter=','):
                    yield row

    def read_participant_oi(self):
        if self._file_participant_oi.is_file():
            with self._file_participant_oi.open('rt') as f:
                csv_reader = csv.reader(f, delimiter=',')
                _ = next(csv_reader, None)
                self.headers_participant_oi = next(csv_reader, None)
                for row in csv.reader(f, delimiter=','):
                    print(row)
                    yield row

    def read_eq_as_pd(self):
        if self.market_close:
            return
        return pd.read_csv(self._file_eq)

    def read_fo_as_pd(self):
        if self.market_close:
            return
        return pd.read_csv(self._file_fo)
