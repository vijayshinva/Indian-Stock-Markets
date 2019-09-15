import sqlite3
import pkgutil
import traceback
import pandas as pd
from pandas.tseries.offsets import BDay
from datetime import date
from indian-stock-markets.nse import BhavCopy

class Nse(object):
    def __init__(self, *args, **kwargs):
        self._create_tables()

    def __enter__(self):
        self.db = sqlite3.connect('nse.db')
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        self.db.commit()
        self.db.close()

    def load(self, start: date, end: date):
        for d in pd.date_range(start, end, freq=BDay()):
            bhavcopy = BhavCopy(d)
            if not bhavcopy.market_close:
                cursor = self.db.cursor()
                cursor.execute('BEGIN TRANSACTION')
                for instrument, symbol, expiry_dt, strike_pr, option_typ, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, timestamp in bhavcopy.read_fo():
                    if instrument == 'FUTSTK':
                        try:
                            cursor.execute('INSERT INTO FUTSTK (SYMBOL,EXPIRY_DT,TIMESTAMP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (symbol, expiry_dt, timestamp, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE FUTSTK SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=?', (open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp))
                    elif instrument == 'FUTIDX':
                        try:
                            cursor.execute('INSERT INTO FUTIDX (SYMBOL,EXPIRY_DT,TIMESTAMP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (symbol, expiry_dt, timestamp, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE FUTIDX SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=?', (open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp))
                    elif instrument == 'OPTSTK':
                        try:
                            cursor.execute('INSERT INTO OPTSTK (SYMBOL,EXPIRY_DT,TIMESTAMP,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (symbol, expiry_dt, timestamp, strike_pr, option_typ, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE OPTSTK SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=? AND STRIKE_PR=? AND OPTION_TYP=?', (open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp, strike_pr, option_typ))
                    elif instrument == 'OPTIDX':
                        try:
                            cursor.execute('INSERT INTO OPTIDX (SYMBOL,EXPIRY_DT,TIMESTAMP,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (symbol, expiry_dt, timestamp, strike_pr, option_typ, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE OPTIDX SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=?  AND TIMESTAMP=? AND STRIKE_PR=? AND OPTION_TYP=?', (open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp, strike_pr, option_typ))
                cursor.execute('COMMIT')

    def _create_tables(self):
        sql_script = pkgutil.get_data(__package__, 'nse.sql').decode('utf-8')
        self.db = sqlite3.connect('nse.db')
        cursor = self.db.cursor()
        cursor.executescript(sql_script)
        self.db.commit()
        self.db.close()
