import unittest
from messari.etherscan import Etherscan
from typing import Dict
import os
import sys
import pandas as pd
import time


class TestEtherscan(unittest.TestCase):
    """This is a unit testing class for testing the Solscan class"""

    def test_init(self):
        """Test initializing Messari class"""
        es = Etherscan()
        self.assertIsInstance(es, Etherscan)

if __name__ == '__main__':
    unittest.main()
