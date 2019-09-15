import sqlite3
import traceback
from datetime import date

class FutStk(object):
    """description of class"""
    def __init__(self, symbol: str, expiry: date):
        self.symbol = symbol
        self.expiry = expiry

    def __enter__(self):
        self.db = sqlite3.connect('nse.db')
        return self
    
    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        self.db.commit()
        self.db.close()

    def open(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT OPEN FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for open in cursor:
            yield open[0]

    def high(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT HIGH FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for high in cursor:
            yield high[0]

    def low(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT LOW FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for low in cursor:
            yield low[0]

    def close(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT CLOSE FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for close in cursor:
            yield close[0]

    def open_interest(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT OPEN_INT FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for open_interest in cursor:
            yield open_interest[0]

    def Volume(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT CONTRACTS FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for volume in cursor:
            yield volume[0]

    def change_open_interest(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT CHG_IN_OI FROM FUTSTK WHERE SYMBOL=? AND EXPIRY_DT=?',(self.symbol, self.expiry))
        for chg_in_oi in cursor:
            yield chg_in_oi[0]