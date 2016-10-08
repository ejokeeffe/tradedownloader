# -*- coding: utf-8 -*-
"""
Created on Fri Oct 03 14:28:25 2014

@author: Eoin O'Keeffe
"""

import getTradeDataFromComtrade

comapi=getTradeDataFromComtrade.ComtradeApi()
df=comapi.getComtradeData()
print(df.head)
