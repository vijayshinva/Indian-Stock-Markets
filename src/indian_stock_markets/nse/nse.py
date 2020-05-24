import sqlite3
import pkgutil
import traceback
import pandas as pd
from pandas.tseries.offsets import BDay
from datetime import date
from indian_stock_markets.nse import BhavCopy


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
                            cursor.execute('INSERT INTO FUTSTK (SYMBOL,EXPIRY_DT,TIMESTAMP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (
                                symbol, expiry_dt, timestamp, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE FUTSTK SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=?', (
                                open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp))
                    elif instrument == 'FUTIDX':
                        try:
                            cursor.execute('INSERT INTO FUTIDX (SYMBOL,EXPIRY_DT,TIMESTAMP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (
                                symbol, expiry_dt, timestamp, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE FUTIDX SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=?', (
                                open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp))
                    elif instrument == 'OPTSTK':
                        try:
                            cursor.execute('INSERT INTO OPTSTK (SYMBOL,EXPIRY_DT,TIMESTAMP,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                symbol, expiry_dt, timestamp, strike_pr, option_typ, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE OPTSTK SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=? AND TIMESTAMP=? AND STRIKE_PR=? AND OPTION_TYP=?', (
                                open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp, strike_pr, option_typ))
                    elif instrument == 'OPTIDX':
                        try:
                            cursor.execute('INSERT INTO OPTIDX (SYMBOL,EXPIRY_DT,TIMESTAMP,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                symbol, expiry_dt, timestamp, strike_pr, option_typ, open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi))
                        except sqlite3.IntegrityError as e:
                            cursor.execute('UPDATE OPTIDX SET OPEN=?,HIGH=?,LOW=?,CLOSE=?,SETTLE_PR=?,CONTRACTS=?,VAL_INLAKH=?,OPEN_INT=?,CHG_IN_OI=? WHERE SYMBOL=? AND EXPIRY_DT=?  AND TIMESTAMP=? AND STRIKE_PR=? AND OPTION_TYP=?', (
                                open, high, low, close, settle_pr, contracts, val_inlakh, open_int, chg_in_oi, symbol, expiry_dt, timestamp, strike_pr, option_typ))
                
                for client_type, future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts in bhavcopy.read_participant_oi():
                    try:
                        if client_type == 'Client':
                            cursor.execute('INSERT INTO CLIENT (OI_FUTURE_INDEX_LONG, OI_FUTURE_INDEX_SHORT, OI_FUTURE_STOCK_LONG, OI_FUTURE_STOCK_SHORT, OI_OPTION_INDEX_CALL_LONG, OI_OPTION_INDEX_PUT_LONG, OI_OPTION_INDEX_CALL_SHORT, OI_OPTION_INDEX_PUT_SHORT, OI_OPTION_STOCK_CALL_LONG, OI_OPTION_STOCK_PUT_LONG, OI_OPTION_STOCK_CALL_SHORT, OI_OPTION_STOCK_PUT_SHORT, OI_TOTAL_LONG_CONTRACTS, OI_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'DII':
                            cursor.execute('INSERT INTO DII (OI_FUTURE_INDEX_LONG, OI_FUTURE_INDEX_SHORT, OI_FUTURE_STOCK_LONG, OI_FUTURE_STOCK_SHORT, OI_OPTION_INDEX_CALL_LONG, OI_OPTION_INDEX_PUT_LONG, OI_OPTION_INDEX_CALL_SHORT, OI_OPTION_INDEX_PUT_SHORT, OI_OPTION_STOCK_CALL_LONG, OI_OPTION_STOCK_PUT_LONG, OI_OPTION_STOCK_CALL_SHORT, OI_OPTION_STOCK_PUT_SHORT, OI_TOTAL_LONG_CONTRACTS, OI_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'FII':
                            cursor.execute('INSERT INTO FII (OI_FUTURE_INDEX_LONG, OI_FUTURE_INDEX_SHORT, OI_FUTURE_STOCK_LONG, OI_FUTURE_STOCK_SHORT, OI_OPTION_INDEX_CALL_LONG, OI_OPTION_INDEX_PUT_LONG, OI_OPTION_INDEX_CALL_SHORT, OI_OPTION_INDEX_PUT_SHORT, OI_OPTION_STOCK_CALL_LONG, OI_OPTION_STOCK_PUT_LONG, OI_OPTION_STOCK_CALL_SHORT, OI_OPTION_STOCK_PUT_SHORT, OI_TOTAL_LONG_CONTRACTS, OI_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'Pro':
                            cursor.execute('INSERT INTO PRO (OI_FUTURE_INDEX_LONG, OI_FUTURE_INDEX_SHORT, OI_FUTURE_STOCK_LONG, OI_FUTURE_STOCK_SHORT, OI_OPTION_INDEX_CALL_LONG, OI_OPTION_INDEX_PUT_LONG, OI_OPTION_INDEX_CALL_SHORT, OI_OPTION_INDEX_PUT_SHORT, OI_OPTION_STOCK_CALL_LONG, OI_OPTION_STOCK_PUT_LONG, OI_OPTION_STOCK_CALL_SHORT, OI_OPTION_STOCK_PUT_SHORT, OI_TOTAL_LONG_CONTRACTS, OI_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                    except sqlite3.IntegrityError as e:
                        if client_type == 'Client':
                            cursor.execute('UPDATE CLIENT SET OI_FUTURE_INDEX_LONG=?, OI_FUTURE_INDEX_SHORT=?, OI_FUTURE_STOCK_LONG=?, OI_FUTURE_STOCK_SHORT=?, OI_OPTION_INDEX_CALL_LONG=?, OI_OPTION_INDEX_PUT_LONG=?, OI_OPTION_INDEX_CALL_SHORT=?, OI_OPTION_INDEX_PUT_SHORT=?, OI_OPTION_STOCK_CALL_LONG=?, OI_OPTION_STOCK_PUT_LONG=?, OI_OPTION_STOCK_CALL_SHORT=?, OI_OPTION_STOCK_PUT_SHORT=?, OI_TOTAL_LONG_CONTRACTS=?, OI_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'DII':
                            cursor.execute('UPDATE DII SET OI_FUTURE_INDEX_LONG=?, OI_FUTURE_INDEX_SHORT=?, OI_FUTURE_STOCK_LONG=?, OI_FUTURE_STOCK_SHORT=?, OI_OPTION_INDEX_CALL_LONG=?, OI_OPTION_INDEX_PUT_LONG=?, OI_OPTION_INDEX_CALL_SHORT=?, OI_OPTION_INDEX_PUT_SHORT=?, OI_OPTION_STOCK_CALL_LONG=?, OI_OPTION_STOCK_PUT_LONG=?, OI_OPTION_STOCK_CALL_SHORT=?, OI_OPTION_STOCK_PUT_SHORT=?, OI_TOTAL_LONG_CONTRACTS=?, OI_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'FII':
                            cursor.execute('UPDATE FII SET OI_FUTURE_INDEX_LONG=?, OI_FUTURE_INDEX_SHORT=?, OI_FUTURE_STOCK_LONG=?, OI_FUTURE_STOCK_SHORT=?, OI_OPTION_INDEX_CALL_LONG=?, OI_OPTION_INDEX_PUT_LONG=?, OI_OPTION_INDEX_CALL_SHORT=?, OI_OPTION_INDEX_PUT_SHORT=?, OI_OPTION_STOCK_CALL_LONG=?, OI_OPTION_STOCK_PUT_LONG=?, OI_OPTION_STOCK_CALL_SHORT=?, OI_OPTION_STOCK_PUT_SHORT=?, OI_TOTAL_LONG_CONTRACTS=?, OI_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'Pro':
                            cursor.execute('UPDATE PRO SET OI_FUTURE_INDEX_LONG=?, OI_FUTURE_INDEX_SHORT=?, OI_FUTURE_STOCK_LONG=?, OI_FUTURE_STOCK_SHORT=?, OI_OPTION_INDEX_CALL_LONG=?, OI_OPTION_INDEX_PUT_LONG=?, OI_OPTION_INDEX_CALL_SHORT=?, OI_OPTION_INDEX_PUT_SHORT=?, OI_OPTION_STOCK_CALL_LONG=?, OI_OPTION_STOCK_PUT_LONG=?, OI_OPTION_STOCK_CALL_SHORT=?, OI_OPTION_STOCK_PUT_SHORT=?, OI_TOTAL_LONG_CONTRACTS=?, OI_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))

                for client_type, future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts in bhavcopy.read_participant_oi():
                    try:
                        if client_type == 'Client':
                            cursor.execute('INSERT INTO CLIENT (VOL_FUTURE_INDEX_LONG, VOL_FUTURE_INDEX_SHORT, VOL_FUTURE_STOCK_LONG, VOL_FUTURE_STOCK_SHORT, VOL_OPTION_INDEX_CALL_LONG, VOL_OPTION_INDEX_PUT_LONG, VOL_OPTION_INDEX_CALL_SHORT, VOL_OPTION_INDEX_PUT_SHORT, VOL_OPTION_STOCK_CALL_LONG, VOL_OPTION_STOCK_PUT_LONG, VOL_OPTION_STOCK_CALL_SHORT, VOL_OPTION_STOCK_PUT_SHORT, VOL_TOTAL_LONG_CONTRACTS, VOL_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'DII':
                            cursor.execute('INSERT INTO DII (VOL_FUTURE_INDEX_LONG, VOL_FUTURE_INDEX_SHORT, VOL_FUTURE_STOCK_LONG, VOL_FUTURE_STOCK_SHORT, VOL_OPTION_INDEX_CALL_LONG, VOL_OPTION_INDEX_PUT_LONG, VOL_OPTION_INDEX_CALL_SHORT, VOL_OPTION_INDEX_PUT_SHORT, VOL_OPTION_STOCK_CALL_LONG, VOL_OPTION_STOCK_PUT_LONG, VOL_OPTION_STOCK_CALL_SHORT, VOL_OPTION_STOCK_PUT_SHORT, VOL_TOTAL_LONG_CONTRACTS, VOL_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'FII':
                            cursor.execute('INSERT INTO FII (VOL_FUTURE_INDEX_LONG, VOL_FUTURE_INDEX_SHORT, VOL_FUTURE_STOCK_LONG, VOL_FUTURE_STOCK_SHORT, VOL_OPTION_INDEX_CALL_LONG, VOL_OPTION_INDEX_PUT_LONG, VOL_OPTION_INDEX_CALL_SHORT, VOL_OPTION_INDEX_PUT_SHORT, VOL_OPTION_STOCK_CALL_LONG, VOL_OPTION_STOCK_PUT_LONG, VOL_OPTION_STOCK_CALL_SHORT, VOL_OPTION_STOCK_PUT_SHORT, VOL_TOTAL_LONG_CONTRACTS, VOL_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'Pro':
                            cursor.execute('INSERT INTO PRO (VOL_FUTURE_INDEX_LONG, VOL_FUTURE_INDEX_SHORT, VOL_FUTURE_STOCK_LONG, VOL_FUTURE_STOCK_SHORT, VOL_OPTION_INDEX_CALL_LONG, VOL_OPTION_INDEX_PUT_LONG, VOL_OPTION_INDEX_CALL_SHORT, VOL_OPTION_INDEX_PUT_SHORT, VOL_OPTION_STOCK_CALL_LONG, VOL_OPTION_STOCK_PUT_LONG, VOL_OPTION_STOCK_CALL_SHORT, VOL_OPTION_STOCK_PUT_SHORT, VOL_TOTAL_LONG_CONTRACTS, VOL_TOTAL_SHORT_CONTRACTS, TRADE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                    except sqlite3.IntegrityError as e:
                        if client_type == 'Client':
                            cursor.execute('UPDATE CLIENT SET VOL_FUTURE_INDEX_LONG=?, VOL_FUTURE_INDEX_SHORT=?, VOL_FUTURE_STOCK_LONG=?, VOL_FUTURE_STOCK_SHORT=?, VOL_OPTION_INDEX_CALL_LONG=?, VOL_OPTION_INDEX_PUT_LONG=?, VOL_OPTION_INDEX_CALL_SHORT=?, VOL_OPTION_INDEX_PUT_SHORT=?, VOL_OPTION_STOCK_CALL_LONG=?, VOL_OPTION_STOCK_PUT_LONG=?, VOL_OPTION_STOCK_CALL_SHORT=?, VOL_OPTION_STOCK_PUT_SHORT=?, VOL_TOTAL_LONG_CONTRACTS=?, VOL_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'DII':
                            cursor.execute('UPDATE DII SET VOL_FUTURE_INDEX_LONG=?, VOL_FUTURE_INDEX_SHORT=?, VOL_FUTURE_STOCK_LONG=?, VOL_FUTURE_STOCK_SHORT=?, VOL_OPTION_INDEX_CALL_LONG=?, VOL_OPTION_INDEX_PUT_LONG=?, VOL_OPTION_INDEX_CALL_SHORT=?, VOL_OPTION_INDEX_PUT_SHORT=?, VOL_OPTION_STOCK_CALL_LONG=?, VOL_OPTION_STOCK_PUT_LONG=?, VOL_OPTION_STOCK_CALL_SHORT=?, VOL_OPTION_STOCK_PUT_SHORT=?, VOL_TOTAL_LONG_CONTRACTS=?, VOL_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'FII':
                            cursor.execute('UPDATE FII SET VOL_FUTURE_INDEX_LONG=?, VOL_FUTURE_INDEX_SHORT=?, VOL_FUTURE_STOCK_LONG=?, VOL_FUTURE_STOCK_SHORT=?, VOL_OPTION_INDEX_CALL_LONG=?, VOL_OPTION_INDEX_PUT_LONG=?, VOL_OPTION_INDEX_CALL_SHORT=?, VOL_OPTION_INDEX_PUT_SHORT=?, VOL_OPTION_STOCK_CALL_LONG=?, VOL_OPTION_STOCK_PUT_LONG=?, VOL_OPTION_STOCK_CALL_SHORT=?, VOL_OPTION_STOCK_PUT_SHORT=?, VOL_TOTAL_LONG_CONTRACTS=?, VOL_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                        if client_type == 'Pro':
                            cursor.execute('UPDATE PRO SET VOL_FUTURE_INDEX_LONG=?, VOL_FUTURE_INDEX_SHORT=?, VOL_FUTURE_STOCK_LONG=?, VOL_FUTURE_STOCK_SHORT=?, VOL_OPTION_INDEX_CALL_LONG=?, VOL_OPTION_INDEX_PUT_LONG=?, VOL_OPTION_INDEX_CALL_SHORT=?, VOL_OPTION_INDEX_PUT_SHORT=?, VOL_OPTION_STOCK_CALL_LONG=?, VOL_OPTION_STOCK_PUT_LONG=?, VOL_OPTION_STOCK_CALL_SHORT=?, VOL_OPTION_STOCK_PUT_SHORT=?, VOL_TOTAL_LONG_CONTRACTS=?, VOL_TOTAL_SHORT_CONTRACTS=? WHERE TRADE_DATE=?', (
                                future_index_long, future_index_short, future_stock_long, future_stock_short, option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short, option_stock_call_long, option_stock_put_long, option_stock_call_short, optoin_stock_put_short, total_long_contracts, total_short_contracts, bhavcopy.date.strftime('%Y-%m-%d')))
                cursor.execute('COMMIT'), total_long_contracts, total_short_contracts

    def _create_tables(self):
        sql_script = pkgutil.get_data(__package__, 'nse.sql').decode('utf-8')
        self.db = sqlite3.connect('nse.db')
        cursor = self.db.cursor()
        cursor.executescript(sql_script)
        self.db.commit()
        self.db.close()
