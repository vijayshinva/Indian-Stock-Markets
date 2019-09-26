from indian_stock_markets.nse import Nse
from datetime import date
import sqlite3
import unittest
import sys
sys.path.append(".")


class TestFutStk(unittest.TestCase):

    def test_tables(self):
        with Nse() as nse:
            db = sqlite3.connect('nse.db')
            cursor = db.cursor()
            cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
            tables = cursor.fetchall()
            self.assertTrue(any('FUTIDX' in t for t in tables))
            self.assertTrue(any('FUTSTK' in t for t in tables))
            self.assertTrue(any('OPTIDX' in t for t in tables))
            self.assertTrue(any('OPTSTK' in t for t in tables))
            self.assertTrue(any('SHORTSELLING' in t for t in tables))
            cursor.close()
            db.close()

    def test_load(self):
        with Nse() as nse:
            nse.load(date(2019, 9, 23), date(2019, 9, 24))

    @classmethod
    def tearDownClass(self):
        pass