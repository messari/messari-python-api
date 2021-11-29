import unittest
from messari.solscan import Solscan
from typing import Dict
import os
import sys
import pandas as pd
import time

class TestSolscan(unittest.TestCase):
    """This is a unit testing class for testing the Solscan class"""

    def test_init(self):
        """Test initializing Messari class"""
        ss = Solscan()
        self.assertIsInstance(ss, Solscan)

if __name__ == '__main__':
    unittest.main()
