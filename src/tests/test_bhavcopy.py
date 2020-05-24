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
                         'https://www1.nseindia.com/content/historical/EQUITIES/2019/SEP/cm25SEP2019bhav.csv.zip')
        self.assertEqual(self.bhavcopy._url_fo.geturl(),
                         'https://www1.nseindia.com/content/historical/DERIVATIVES/2019/SEP/fo25SEP2019bhav.csv.zip')
        self.assertEqual(self.bhavcopy._url_short_selling.geturl(),
                         'https://www1.nseindia.com/archives/equities/shortSelling/shortselling_25092019.csv')
        self.assertEqual(self.bhavcopy._url_participant_oi.geturl(),
                         'https://www1.nseindia.com/content/nsccl/fao_participant_oi_25092019.csv')
        self.assertEqual(self.bhavcopy._url_participant_vol.geturl(),
                         'https://www1.nseindia.com/content/nsccl/fao_participant_vol_25092019.csv')

    def test_downloaded_files(self):
        eq_path = Path(self.bhavcopy._url_eq.path[1:])
        fo_path = Path(self.bhavcopy._url_fo.path[1:])
        short_selling_path = Path(self.bhavcopy._url_short_selling.path[1:])
        participant_oi_path = Path(self.bhavcopy._url_participant_oi.path[1:])
        participant_vol_path = Path(self.bhavcopy._url_participant_vol.path[1:])
        self.assertTrue(eq_path.is_file())
        self.assertTrue(fo_path.is_file())
        self.assertTrue(short_selling_path.is_file())
        self.assertTrue(participant_oi_path.is_file())
        self.assertTrue(participant_vol_path.is_file())
