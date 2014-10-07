tradedownloader
===============

Download trade data from Comtrade using the exposed Comtrade API.

Requirements
============
Python 2.7 (only tested on this version, it may work on others)
Pandas


Functioning
===========
The code incrementally builds a trade database with each call for trade. Any missing data is retrieved from the Comtrade API and added to the database. The database currently is simply a series of csv files. Most likely this will be extend to proper database support. 

Includes:
=========
Comtrade country codes file for resolving country names
