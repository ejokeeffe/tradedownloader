import pytest
from tradedownloader import utilities
import pandas as pd
import numpy


__author__ = "Eoin O'Keeffe"
__copyright__ = "Eoin O'Keeffe"
__license__ = "none"


class TestUtilities():
    def testRemoveSpurious():
        """Tests that the filtering criteria is working correctly"""

        # Quick and simple test on outlier - 1 obs should 
        # be removed
        df_test=pd.DataFrame({'cmdCode':numpy.zeros(5),'ptCode':numpy.ones(5),'rtCode':numpy.ones(5)*2})
        df_test['NetWeight']=[0,1,1,0,10]
        df_res=utilities.remove_spurious(df_test)
        assert 4==df_res.shape[0]