from datetime import date
from indian_stock_markets.nse import BhavCopy
from pathlib import Path
import unittest
import sys
sys.path.append(".")


class TestBhavCopy(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bhavcopy = BhavCopy(date(2019, 9, 25))

    def test_download_urls(self):
        self.assertEqual(self.bhavcopy._url_eq.geturl(),
                         'https://www.nseindia.com/content/historical/EQUITIES/2019/SEP/cm25SEP2019bhav.csv.zip')
        self.assertEqual(self.bhavcopy._url_fo.geturl(),
                         'https://www.nseindia.com/content/historical/DERIVATIVES/2019/SEP/fo25SEP2019bhav.csv.zip')
        self.assertEqual(self.bhavcopy._url_short_selling.geturl(),
                         'https://www.nseindia.com/archives/equities/shortSelling/shortselling_25092019.csv')

    def test_downloaded_files(self):
        eq_path = Path(self.bhavcopy._url_eq.path[1:])
        fo_path = Path(self.bhavcopy._url_fo.path[1:])
        short_selling_path = Path(self.bhavcopy._url_short_selling.path[1:])
        self.assertTrue(eq_path.is_file())
        self.assertTrue(fo_path.is_file())
        self.assertTrue(short_selling_path.is_file())

    @classmethod
    def tearDownClass(self):
        eq_path = Path(self.bhavcopy._url_eq.path[1:])
        fo_path = Path(self.bhavcopy._url_fo.path[1:])
        short_selling_path = Path(self.bhavcopy._url_short_selling.path[1:])
        #eq_path.unlink()
        #fo_path.unlink()
        #short_selling_path.unlink()
