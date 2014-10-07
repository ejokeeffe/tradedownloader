tradedownloader
===============

Download trade data from Comtrade using the exposed Comtrade API.

Requirements
============
Python 2.7 (only tested on this version, it may work on others)
Pandas

Usage
=====
API details are shown here: http://comtrade.un.org/data/doc/api/
Most notably, there is a rate limit of 1 request per second, 100 requests per hour.
Note that the api may change in future - please don't abuse the api. The purpose of this code is to prevent unnecessary calls to the api, not to aggregate all the comtrade data (they have a strict policy on redistribution also - http://comtrade.un.org/db/help/PolicyOnUseAndRedissemination.pdf)



Functioning
===========
The code incrementally builds a trade database with each call for trade. Any missing data is retrieved from the Comtrade API and added to the database. The database currently is simply a series of csv files. Most likely this will be extend to proper database support. 

API calls must be split as it's not possible to call more than 5 commodity codes at a time or more than 5 countries at a time. Therefore, any calls involving "all countries" must be split groupings of 5 countrires (hence inclusion of comtrade country codes file). There is a random delay of greater than 1 second applied before each call. 

Example run is shown in test.py. The option fields in the call are shown in the file and below: 
 # @param comcodes - array/list of HS country codes as strings
 # @param reporter - array/list of comtrade countries as list or use 'all' for
    # all countries
    # @param partner - same as above
    # @param years -array of years as integers
    # @param freq - 'A' or 'M' for year or monthly data respectively
    # @param rg='1' for imports and '2' for exports - only tested for imports
    # @param fmt='json' - default
    #@param rowmax=50000. No reason to change this. This is the max value.


Includes:
=========
Comtrade country codes file for resolving country names

How to Install
=============
1. clone or download the files
2. Run test.py to ensure it works. If it doesn't, check all the modules required are installed, otherwise get in touch - it may be a bug.
3. That's it. Copy the test.py code into your own python files or extend it. 
