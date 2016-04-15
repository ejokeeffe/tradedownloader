
import logging
import pandas
import numpy
import math

def remove_spurious(df=None):
    """Given a comtrade O-D series, removes 
    any spurious datapoints. This is basically where 
    chauvrenets criterion is invalidated

    This function doesn't owrk for trade. As they're time series
    it's ripping out too many points - and not hte right ones

    Need to probably generate a naive bayes model and train it
    

    Arguments
    ---------
    df: DataFrame
        comtrade dataset

    Returns
    -------
    df: DataFrame
        same as input but with observation removed
    """
    df.reset_index(inplace=True, drop=True)
    drop_indices = []
    ctries = df.ptCode.unique()
    # ctries = numpy.hstack((ctries, df.rtCode.unique()))
    # ctries = numpy.unique(ctries)
    # [src, dst] = numpy.meshgrid(ctries, ctries)
    # src = numpy.ravel(src)
    # dst = numpy.ravel(dst)
    for jj, com in enumerate(df.cmdCode.unique()):
        logging.debug("Removing spurious data for com {0} of {1}".
                      format(jj, len(df.cmdCode.unique())))
        df_com=df.ix[(df.cmdCode == com)]
        # get the criterion
        mu = df_com['NetWeight'].mean()
        std = df_com['NetWeight'].std()
        p_d = [numpy.power(std * numpy.power(2 * numpy.pi, 0.5), -1) *
               numpy.exp(numpy.power(x - mu, 2) /
                         (2 * numpy.power(std, 2)))
               for x in df_com['NetWeight'].values]
        check_indices=df_com.ix[
                        [x * df_com.shape[0] >= 0.5 for x in p_d]].index.values
        for imp,exp in df.ix[check_indices,['rtCode','ptCode']].drop_duplicates().values:
            df_sel = df.ix[(df.cmdCode == com) &
                           (df.rtCode == imp) &
                           (df.ptCode == exp)]
            mu = df_sel['NetWeight'].mean()
            std = df_sel['NetWeight'].std()
            # p_d = [numpy.power(std * numpy.power(2 * numpy.pi, 0.5), -1) *
            #        numpy.exp(numpy.power(x - mu, 2) /
            #                  (2 * numpy.power(std, 2)))
            #        for x in df_sel['NetWeight'].values]
            p_d=[math.erfc((x-mu)/(std*numpy.sqrt(2))) for x in df_sel['NetWeight']]

            drop_indices.append(df_sel.ix[
                [x * df_sel.shape[0] < 0.5 for x in p_d]].index.values)

    # Now remove the indices
    df=df.copy()
    for rem in drop_indices:
        df.drop(rem, inplace=True, axis=0)
    return df
