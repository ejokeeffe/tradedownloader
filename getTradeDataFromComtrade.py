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


class ComtradeApi:
    _url='http://comtrade.un.org/api/get?'
    _ctry_codes=[]
    _max_partners=5
    _max_years=5
    _last_call_time=0
    _working_df=[]
    """
    Pure api call, no updating of a database
    
    
    """
    def __init__(self,ctry_codes_path="UN Comtrade Country List.csv"):
        #load the country codes
        self._ctry_codes=pd.read_csv(ctry_codes_path)
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['End Valid Year']>2012]
        # Remove NES and other areas
        #World
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=0]
        #EU-27
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=97]
        #LAIA NES
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=473]
        #Oceania NES
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=527]
        #Europe NES
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=568]
        #Other Africa NES
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=577]
        #Bunkers
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=837]
        #Free Zones
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=838]
        #Special Caegories
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=839]
        #Areas NES
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=899]
        #Neutral Zone
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=536]
        #North America and Central America, nes
        self._ctry_codes=self._ctry_codes.ix[self._ctry_codes['ctyCode']!=637]
        #Finally remove where the iso codes are not available
        self._ctry_codes=self._ctry_codes.ix[pd.isnull(self._ctry_codes['ISO2-digit Alpha'])==False]
        self._ctry_codes=self._ctry_codes.ix[pd.isnull(self._ctry_codes['ISO3-digit Alpha'])==False]
        
        saved_queries_fname=join("saved_queries.csv")
        if (os.path.isfile(saved_queries_fname)):
            self._saved_queries=pd.read_csv(saved_queries_fname)
        else:
            self._saved_queries=pd.DataFrame({'id':[],'querystring':[],\
                'comcode':[],'year':[],'freq':[]})
        self._saved_queries.comcode=self._saved_queries.comcode.astype(pd.np.str)
        self._saved_queries.freq=self._saved_queries.freq.astype(pd.np.str)
        self._saved_queries.querystring=self._saved_queries.querystring.astype(pd.np.str)
        
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
    def getComtradeData(self,comcodes=['1201'],reporter=['all'],partner=['all'],\
        years=[2012],freq='A',rg=['1'],fmt='json',rowmax=50000):
        
        
        #build string
        s=self._url + "type=C"
        s="%s&freq=%s"%(s,freq)
        s="%s&px=HS"%s
        if freq=='A':
                s="%s&ps=%s"%(s,"%2C".join([str(yr) for yr in years]))   
        else:
            mons=[["%s%02d"%(yr,mon) for mon in range(1,13)] for yr in years]
            s="%s&ps=%s"%(s,"%2C".join(mons[0]))
        
        s="%s&r=%s"%(s,"%2C".join(reporter))
        
        # For large queries, it's possible we've stored the results
        if reporter[0]=="all" and partner[0]=="all":
            ##Check if query has already been done
            df_base=[]
            retain=[]
            load_files=[]
            for yr in years:
                haveit=True
                for com in comcodes:
                    if len(self._saved_queries.ix[(self._saved_queries.comcode==com) &\
                        (self._saved_queries.year==yr)&(self._saved_queries.freq==freq)])==0:
                            print "Can't find %d and %s" %(yr,com)
                            retain.append(yr)
                            haveit=False
                            break
                if haveit==True:
                    #print "we have it"
                    for com in comcodes:
                        fname=self._saved_queries.id.ix[(self._saved_queries.comcode==com) &\
                            (self._saved_queries.year==yr)&(self._saved_queries.freq==freq)].values[0]
                        load_files.append(fname)
            load_files=numpy.unique(load_files)
            for fname in load_files:
                fname=join("%d.csv"%fname)
                df_new=pd.read_csv(fname)
                #only keep current year
                if len(df_base)==0:
                    df_base=df_new
                else:
                    df_base=df_base.append(df_new)
                    df_base.drop_duplicates(inplace=True)
                    #Remove the year from years
                    #for_removal.append(years.index(yr))
    
            filter_years=years
            years=retain
            if (len(years)==0):
                #print "filled up so jumping out"
                #jump out
                df_base.drop_duplicates(inplace=True)
                #Only return wanted commodity codes
                #print df_base.head()
                df_base=df_base.ix[df_base.cmdCode.isin([int(numeric_string) for numeric_string in comcodes])]
                if freq=='A':
                    df_base=df_base.ix[df_base.period.isin(filter_years)]
                    #df_base=df_base.ix[df_base.period.isin(years)]
                return df_base
            df=[]
            #can't pass both as all, iterate through country codes and add each
            #Need to split into calls of 
            for start_val in range(0,len(self._ctry_codes.ctyCode),self._max_partners):
                print "Running %d of %d"%(start_val,len(self._ctry_codes.ctyCode))
                for start_year in range(0,len(years),self._max_years):
                    end_val=min([start_val+self._max_partners,len(self._ctry_codes.ctyCode)])
                    end_year=min([start_year+self._max_years,len(years)])
                    print "%d,%d"%(start_year,end_year)
                    sub_partners=self._ctry_codes.ctyCode.values[start_val:end_val]
                    sub_partners=[str(com) for com in sub_partners]
                    #print sub_partners
                    if len(df)==0:
                        df=self.getComtradeData(comcodes=comcodes,reporter=reporter,partner=sub_partners,\
                        years=years[start_year:end_year],freq=freq,rg=rg,fmt=fmt,rowmax=rowmax)
                    else:
                        #pass
                        df=df.append(self.getComtradeData(comcodes=comcodes,reporter=reporter,partner=sub_partners,\
                            years=years[start_year:end_year],freq=freq,rg=rg,fmt=fmt,rowmax=rowmax))
                        #if start_val>40:
                        #    break
            
             #save this query to disk
            id=0
            if (len(self._saved_queries.id)==0):
                id =0
            else:
                id=max(self._saved_queries.id.values)+1            
            print "new id:%d" %id
            for yr in years:
                for com in comcodes:
                    
                    df_new=pd.DataFrame({'id':[id],'querystring':[s],\
                        'comcode':[com],'year':[yr],'freq':[freq]})
                    self._saved_queries=self._saved_queries.append(df_new)
            #unencode from unicode
            
            df.rtTitle=df.rtTitle.apply(lambda x: \
                unicodedata.normalize('NFKD', x).encode('ascii','ignore'))
            df.ptTitle=df.ptTitle.apply(lambda x: \
                unicodedata.normalize('NFKD', x).encode('ascii','ignore'))
            self._working_df=df
            #write to file
            self._saved_queries.to_csv(join("saved_queries.csv"),index=None)
            self._working_df=df
            df.to_csv(join("%d.csv"%id),index=None)
        
            if len(df_base)>0:
                df.append(df_base)
                ##keep unique rows
                df.drop_duplicates(inplace=True)
            #Only return wanted commodity codes
            df=df.ix[df.cmdCode.isin([int(numeric_string) for numeric_string in comcodes])]
            if freq=='A':
                    df=df.ix[df.period.isin(filter_years)]
            return df   
        else:
            s="%s&p=%s"%(s,"%2C".join(partner))
        s="%s&rg=%s"%(s,"%2C".join(rg))
        s="%s&cc=%s"%(s,"%2C".join(comcodes))
        s="%s&fmt=%s"%(s,fmt)
        s="%s&max=%d"%(s,rowmax)
        s="%s&head=M"%(s)
        #print "Doing an api call"
        print s
        waiting_time=round(time.time() * 1000)-self._last_call_time
        if (waiting_time<1000):
            #print waiting_time
            #print("Sleeping for %.0f"%(1000-waiting_time))
            time.sleep(float(1000-waiting_time)/1000)
        r=requests.get(r'%s'%(s))
        self._last_call_time = round(time.time() * 1000.0)
        #print self._last_call_time       
        data=r.json()
        df=pd.DataFrame(data['dataset'])
        print "Returned rows: %d" %df.shape[0]

        return df
