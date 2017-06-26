# -*- coding: utf-8 -*-
"""
This generates and calls the api

It's worth writing this so that a database is updated - maybe csv based for 
each commodity code

@author: Eoin O'Keeffe

@date 08/08/2014

"""
import requests
import pandas as pd
from os.path import join
import os
import time
import unicodedata
import numpy
import datetime
import sys
from os.path import isfile
import logging


class ComtradeApi:
    _source_folder = ""
    _url = 'http://comtrade.un.org/api/get?'
    _ctry_codes = []
    _ctry_alt_names = []
    _max_partners = 5
    _max_years = 5
    _last_call_time = 0
    _working_df = []
    calls_in_hour = 0
    first_call = datetime.datetime.now()
    max_calls = 95
    """
    Pure api call, no updating of a database
    
    
    """

    def __init__(self, ctry_codes_path="UN Comtrade Country List.csv", fld=""):
        self._source_folder = fld
        # load the country codes
        self._ctry_codes = pd.read_csv(
            join(fld, ctry_codes_path), keep_default_na=False, encoding="ISO-8859-1")
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['End Valid Year'] > 2012]
        # check to make sure there are correct fields
        if ('ISO2-digit Alpha' not in self._ctry_codes.columns) | \
                ('ISO3-digit Alpha' not in self._ctry_codes.columns):
            logging.warning("----------------------------")
            logging.warning("                 ")
            logging.warning("Check format of %s file" % ctry_codes_path)
            logging.warning("It appears to be missing fields")
            logging.warning("                 ")
            logging.warning("--------------------------")
            return
        # Remove NES and other areas
        # check the field name for the country code
        if "Country Code" in self._ctry_codes.columns:
            self._ctry_codes['ctyCode'] = self._ctry_codes['Country Code']
        # World
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 0]
        # EU-27
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 97]
        # LAIA NES
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 473]
        # Oceania NES
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 527]
        # Europe NES
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 568]
        # Other Africa NES
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 577]
        # Bunkers
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 837]
        # Free Zones
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 838]
        # Special Caegories
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 839]
        # Areas NES
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 899]
        # Neutral Zone
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 536]
        # North America and Central America, nes
        self._ctry_codes = self._ctry_codes.ix[
            self._ctry_codes['ctyCode'] != 637]
        # Finally remove where the iso codes are not available
        self._ctry_codes = self._ctry_codes.ix[
            pd.isnull(self._ctry_codes['ISO2-digit Alpha']) == False]
        self._ctry_codes = self._ctry_codes.ix[
            pd.isnull(self._ctry_codes['ISO3-digit Alpha']) == False]

        # now the country code alternative names
        # if this doesn't exist then create it
        if not isfile(join(fld, "country_alternative_names.csv")):
            # doesn't exist so create it
            self._ctry_alt_names = pd.DataFrame({'ISO2-digit Alpha': ['KR'],
                                                 'Aka': ['South Korea']})
            # write this to file
            self._ctry_alt_names.to_csv(
                join(fld, "country_alternative_names.csv"), index=None)
        self._ctry_alt_names = pd.read_csv(join(fld, "country_alternative_names.csv"),
                                           keep_default_na=False)

        saved_queries_fname = join(self._source_folder, "saved_queries.csv")
        if (os.path.isfile(saved_queries_fname)):
            self._saved_queries = pd.read_csv(saved_queries_fname)
        else:
            self._saved_queries = pd.DataFrame({'id': [], 'querystring': [],
                                                'comcode': [], 'year': [], 'freq': []})
        self._saved_queries.comcode = self._saved_queries.comcode.astype(
            pd.np.str)
        self._saved_queries.freq = self._saved_queries.freq.astype(pd.np.str)
        self._saved_queries.querystring = self._saved_queries.querystring.astype(
            pd.np.str)

    #---------------------------------
    #
    # Call this to get data
    #
    # @param comcodes - array/list of HS country codes as strings
    # @param reporter - array/list of comtrade countries as list or use 'all' for
    # all countries
    # @param partner - same as above
    # @param years -array of years as integers
    # @param freq - 'A' or 'M' for year or monthly data respectively
    # @param rg='1' for imports and '2' for exports - only tested for imports
    # @param fmt='json' - default
    #@param rowmax=50000. No reason to change this. This is the max value.
    #----------------------------------
    def getComtradeData(self, comcodes=['1201'], reporter=['all'], partner=['all'],
                        years=[2012], freq='A', rg=['1'], fmt='json', rowmax=50000):

        # build string
        s = self._url + "type=C"
        s = "%s&freq=%s" % (s, freq)
        s = "%s&px=HS" % s
        if freq == 'A':
            s = "%s&ps=%s" % (s, "%2C".join([str(yr) for yr in years]))
        else:
            mons = [["%s%02d" % (yr, mon) for mon in range(1, 13)]
                    for yr in years]
            s = "%s&ps=%s" % (s, "%2C".join(mons[0]))

        s = "%s&r=%s" % (s, "%2C".join(reporter))

        # For large queries, it's possible we've stored the results
        if reporter[0] == "all" and partner[0] == "all":
            # Check if query has already been done
            df_base = []
            retain = []
            load_files = []
            for yr in years:
                haveit = True
                logging.info(self._saved_queries.dtypes)
                logging.info(self._saved_queries.head())
                for com in comcodes:
                    if len(self._saved_queries.ix[([float(x)==float(com) for x in self._saved_queries.comcode.values]) &
                                                  (self._saved_queries.year == yr) & (self._saved_queries.freq == freq)]) == 0:
                        logging.info(
                            "Can't find %d and %s from local database. These will be retrieved from the Comtrade API." % (yr, com))
                        retain.append(yr)
                        haveit = False
                        break
                if haveit == True:
                    # print "we have it"
                    for com in comcodes:
                        fname = self._saved_queries.id.ix[([float(x)==float(com) for x in self._saved_queries.comcode.values]) &
                                                          (self._saved_queries.year == yr) & 
                                                          (self._saved_queries.freq == freq)].values[0]
                        # only load it if fname if >0
                        if (fname > -1):
                            load_files.append(fname)
                        else:
                            logging.info("{} for year {} and freq {} not available in comtrade. Fname: {}".format(
                                com, yr, freq, fname))
            load_files = numpy.unique(load_files)
            for fname in load_files:
                fname = join(self._source_folder, "%d.csv" % fname)
                df_new = pd.read_csv(fname)
                # only keep current year
                if len(df_base) == 0:
                    df_base = df_new
                    df_base.drop_duplicates(inplace=True)
                else:
                    df_base = df_base.append(df_new)
                    df_base.drop_duplicates(inplace=True)
                    # Remove the year from years
                    # for_removal.append(years.index(yr))

                # Now remove years and coms outside the requested
                # logging.info(df_base.shape)
                #df_base=df_base.ix[df_base.yr.apply(lambda x: x in years)].copy()
                # logging.info(df_base.shape)
                #df_base=df_base.ix[df_base.cmdCode.apply(lambda x: x in [int(x) for x in comcodes])].copy()
                # logging.info(df_base.shape)

            filter_years = years
            years = retain
            # logging.debug(years)
            if (len(years) == 0):
                # print "filled up so jumping out"
                # jump out
                df_base.drop_duplicates(inplace=True)
                # Only return wanted commodity codes

                df_base = df_base.ix[df_base.cmdCode.isin(
                    [int(numeric_string) for numeric_string in comcodes])].copy()

                if freq == 'A':
                    # logging.debug(df_base.head())
                    df_base = df_base.ix[df_base.period.isin(filter_years)]
                    df_base['date'] = [datetime.datetime(
                        x, 1, 1) for x in df_base.period]
                if freq == 'M':
                    df_base['year'] = [int(str(x)[:4])
                                       for x in df_base.period.values]
                    df_base = df_base.ix[df_base.year.isin(filter_years)]
                    df_base['date'] = [datetime.datetime(
                        int(str(x)[:4]), int(str(x)[4:]), 1) for x in df_base.period]
                    df_base.drop('year', inplace=True, axis=1)
                # logging.debug(filter_years)
                    # df_base=df_base.ix[df_base.period.isin(years)]
                # reset_index
                df_base.reset_index(drop=True, inplace=True)
                return df_base
            df = []
            # logging.debug(df_base.head())
            # can't pass both as all, iterate through country codes and add each
            # Need to split into calls of
            for start_year in range(0, len(years), self._max_years):
                df_cur_years = []
                for start_val in range(0, len(self._ctry_codes.ctyCode), self._max_partners):
                    end_val = min([start_val + self._max_partners,
                                   len(self._ctry_codes.ctyCode)])
                    end_year = min([start_year + self._max_years, len(years)])
                    # logging.info("%d,%d"%(start_year,end_year))
                    sub_partners = self._ctry_codes.ctyCode.values[
                        start_val:end_val]
                    sub_partners = [str(com) for com in sub_partners]
                    logging.debug("Running {} of {} subqueries for year(s) ".format(
                        start_val, len(self._ctry_codes.ctyCode), years[start_year:end_year]))
                    # print sub_partners
                    new_data = self.getComtradeData(comcodes=comcodes, reporter=reporter, partner=sub_partners,
                                                    years=years[start_year:end_year], freq=freq, rg=rg, fmt=fmt, rowmax=rowmax)
                    if len(df_cur_years) == 0:
                        df_cur_years = new_data
                    else:
                        # pass
                        try:
                            # print "appending..."
                            df_cur_years = df_cur_years.append(new_data)
                        except e:
                            logging.warning(
                                "Error trying to append new service call data")
                            exception_name, exception_value = sys.exc_info()[
                                :2]
                            raise   # or don't -- it's up to you
                            # print "Appending..."

                        # if start_val>40:
                        # break
                # Now save this call to file
                # save this query to disk
                id = 0
                if len(df_cur_years) == 0:
                    id = -1
                elif (len(self._saved_queries.id) == 0):
                    id = 0
                else:
                    id = max(self._saved_queries.id.values) + 1
                # if len(df)>0:
                logging.info(
                    "Writing {}/{} to file with id:{}".format(comcodes, years[start_year:end_year], id))
                for yr in years[start_year:end_year]:
                    for com in comcodes:
                        df_new = pd.DataFrame({'id': [id], 'querystring': [s],
                                               'comcode': [com], 'year': [yr], 'freq': [freq]})
                        self._saved_queries = self._saved_queries.append(
                            df_new)
                self._saved_queries.to_csv(join(self._source_folder,
                                                "saved_queries.csv"), index=None)
                if id > -1:
                    # unencode from unicode
                    df_cur_years.rtTitle = df_cur_years.rtTitle.apply(lambda x:
                                                                      unicodedata.normalize('NFKD', x).encode('ascii', 'ignore'))
                    df_cur_years.ptTitle = df_cur_years.ptTitle.apply(lambda x:
                                                                      unicodedata.normalize('NFKD', x).encode('ascii', 'ignore'))
                    # self._working_df=df_cur_years
                    # write to file

                    # self._working_df=df_cur_years
                    df_cur_years.to_csv(
                        join(self._source_folder, "%d.csv" % id), index=None)

                    # Now merge this with df
                    if len(df) == 0:
                        df = df_cur_years
                    else:
                        try:
                            # print "appending..."
                            df = df.append(df_cur_years)
                        except e:
                            logging.warning("Error trying to append. Years: {}. Com: {}".format(
                                years[start_year:end_year], comcodes))
                            exception_name, exception_value = sys.exc_info()[
                                :2]
                            raise
            logging.debug("Have the merged dataset")

            # print df.head()

            if len(df_base) > 0:
                # print df_base.head()
                df = df.append(df_base)
                # keep unique rows
                # print df.head()
                if len(df) > 0:
                    df.drop_duplicates(inplace=True)
            # Only return wanted commodity codes
            if len(df) > 0:
                # ensure cmdCode is an integer
                df.cmdCode = df.cmdCode.astype(pd.np.int)
                df.period = df.period.astype(pd.np.int)
                # print df.head()
                # print comcodes
                # print filter_years
                # print df.dtypes
                df = df.ix[df.cmdCode.isin(
                    [int(numeric_string) for numeric_string in comcodes])]
                # if freq=='A':
                df = df.ix[df.yr.isin(filter_years)]
                # print df.head()
            return df
        else:
            s = "%s&p=%s" % (s, "%2C".join(partner))
        s = "%s&rg=%s" % (s, "%2C".join(rg))
        s = "%s&cc=%s" % (s, "%2C".join([str(int(x)) for x in comcodes]))
        s = "%s&fmt=%s" % (s, fmt)
        s = "%s&max=%d" % (s, rowmax)
        s = "%s&head=M" % (s)

        # first we need to figure out if we can do the call - we may have
        # exceeded our allowable calls
        if (ComtradeApi.first_call < datetime.datetime.now() + datetime.timedelta(hours=-1)):
            # we've waiting long enough
            ComtradeApi.first_call = datetime.datetime.now()
            ComtradeApi.calls_in_hour = 0
        else:
            # we're within time, so check how many we have left
            if (ComtradeApi.calls_in_hour + 1 < ComtradeApi.max_calls):
                logging.info("you've made %d calls this hour, %d left" %
                             (ComtradeApi.calls_in_hour + 1,
                              ComtradeApi.max_calls - ComtradeApi.calls_in_hour - 1))
                ComtradeApi.calls_in_hour += 1
            else:
                # Need to wait for an hour

                waiting_time = datetime.timedelta(hours=1.05)
                logging.warning("You've exceeded the max calls in an hour, so now you'll have to wait until {}".format(
                    datetime.datetime.now() + waiting_time))
                sys.stdout.flush()
                time.sleep(waiting_time.total_seconds())
                logging.debug("Stepping out of waiting. Time: {}".format(
                    datetime.datetime.now()))
                ComtradeApi.first_call = datetime.datetime.now()
                ComtradeApi.calls_in_hour = 0
        # print "Doing an api call"
        logging.info(s)
        waiting_time = round(time.time() * 1000) - self._last_call_time
        if (waiting_time < 1000):
            # print waiting_time
            #print("Sleeping for %.0f"%(1000-waiting_time))
            time.sleep(float(1000 - waiting_time) / 1000)
        # print(s)
        # hitting issues with the certificate validation, so ignoring it for now
        r = requests.get(r'%s' % (s),verify=False)
        self._last_call_time = round(time.time() * 1000.0)
        # print self._last_call_time
        try:
            data = r.json()
        except:
            logging.warning("No json object to be parsed")
            logging.warning(
                "Trying running the string below in your browser -is there an error?")
            logging.warning("return string: {}".format(r))
            logging.warning("%s" % s)
            exception_name, exception_value = sys.exc_info()[:2]
            logging.warning("{} {}".format(exception_name, exception_value))
            raise

            return []
        df = pd.DataFrame(data['dataset'])
        logging.info("Returned rows: %d" % df.shape[0])

        return df

    @property
    def ctry_codes(self):
        return self._ctry_codes
