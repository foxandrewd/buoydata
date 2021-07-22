"""
Created on: Thursday May 27, 2021
@author: Andrew Fox
"""

import os, yaml
import pandas as pd
import numpy as np
import random as rnd
import datetime as dt
import re


base_folder = 'C:/Users/Barbf/Downloads/cmanwx'

BASIS_YEAR = '2020'
BASIS_MONTH = '01'

MAX_FILES_TO_RUN = 1

OUTPUT_YEARS = [2023]
OUTPUT_MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]

# Zero'th month doesn't exist:
C_MonthLength = [0,31,28,31,30,31,30,31,31,30,31,30,31]


def main():
    
    basis_csv_folder = '/'.join([base_folder,BASIS_YEAR,BASIS_MONTH,'csv'])
    (_,_,files) = next(os.walk(basis_csv_folder))
            
    for month in OUTPUT_MONTHS:
        mon = str(month).zfill(2)
        (_,_,files) = next(os.walk('/'.join([base_folder, BASIS_YEAR, mon, 'csv'])))
        for fname in files[:MAX_FILES_TO_RUN]:
            run_analysis(BASIS_YEAR, fname, BASIS_YEAR, month)
    
    # Generate simulated data for all output years and months:
    for year in OUTPUT_YEARS:
        for month in OUTPUT_MONTHS:
            mon = str(month).zfill(2)
            (_,_,files) = next(os.walk('/'.join([base_folder, BASIS_YEAR, mon, 'csv'])))                    
            for fname in files[:MAX_FILES_TO_RUN]:
                datagen(BASIS_YEAR, fname, year, month)


def run_analysis(basis_yr, fname, year, month):
    yr = str(year)
    mon = str(month).zfill(2)
    yrmon = yr+mon # e.g. '202501'
    df = pd.read_csv(base_folder + '/'+yr+'/'+mon+ "/csv/" + fname, index_col=0)
    
    outcolumns = ['FieldName','DataType','Min','Max','Mean','StdDev','Median','Mode',
                  'NumValues','NumNulls','NumUnique','AutoCorr','FFT','Distrib']
    
    dfout = pd.DataFrame(index=df.columns, columns = outcolumns)
    
    time_gap_hours = (df.index[1] - df.index[0])/3600.0
    
    for col in df:
        if df[col].dtype in ('int64','float64'):
            df[col][df[col] > 1e34] = np.nan
            df[col][df[col] < -1e6] = np.nan
        if "wind_direction." in col:
            df[col][df[col] < -32500] = np.nan
    
    for col in df:
        dfout['FieldName'][col] = col
        dfout['DataType'][col] = df[col].dtype
        try: dfout['Min'][col] = df[col].min()
        except: dfout['Min'][col] = 'NA'
        try: dfout['Max'][col] = df[col].max()
        except: dfout['Max'][col] = 'NA'    
        try: dfout['Mean'][col] = df[col].mean()
        except: dfout['Mean'][col] = 'NA'    
        try: dfout['Median'][col] = df[col].median()
        except: dfout['Median'][col] = 'NA'        
        try: dfout['Mode'][col] = df[col].mode()[0]
        except: dfout['Mode'][col] = 'NA'
        try: dfout['StdDev'][col] = df[col].std()
        except: dfout['StdDev'][col] = 'NA'
        try: dfout['NumValues'][col] = df[col].size
        except: dfout['NumValues'][col] = 'NA'
        try: dfout['NumNulls'][col] = df[col].size - df[col].count()
        except: dfout['NumNulls'][col] = 'NA'
        try: dfout['NumUnique'][col] = df[col].nunique()
        except: dfout['NumUnique'][col] = 'NA'
        try:
            if df[col].nunique() <= 8:
                data_dist  = df[col].value_counts(dropna=False,normalize=True)
                data_vals  = list(data_dist.index)
                data_probs = list(data_dist.values)                
                for i,v in enumerate(data_vals):
                    if str(v).startswith("b'") and str(v).endswith("'"):
                        data_vals[i] = str(v)[2:-1]  # Remove the "b'" prefix and "'" suffix
                data_dist_dict = dict(zip(data_vals, data_probs))
                dfout['Distrib'][col] = str(data_dist_dict)
            else: dfout['Distrib'][col] = {}
        except: dfout['Distrib'][col] = {}
            
        try:    dfout['AutoCorr'][col] = df[col].autocorr()
        except: dfout['AutoCorr'][col] = 'NA'

        if df[col].dtype in ('int64','float64'):
            x = df[col].values
            xi = np.arange(len(x))
            mask = np.isfinite(x)
            xsmoothed = np.interp(xi, xi[mask], x[mask])
            fftvals = np.abs(np.fft.rfft(xsmoothed))
            fftfreq = np.fft.rfftfreq(n=len(x), d=time_gap_hours)
            maxval_position = fftvals.argmax()
            maxval_freq = fftfreq[maxval_position]
            dfout['FFT'][col] = 1/maxval_freq
        else:
            dfout['FFT'][col] = 'NA'
    
    analysis_folder = '/'.join([base_folder,basis_yr,mon,'analysis'])
    os.makedirs(analysis_folder, exist_ok=True)
    dfout.to_csv(analysis_folder + '/' + "analysis_" + fname)
    
def sampleN_data_dist(N, dist):
    dist = yaml.load(dist)
    print('distrib: ', dist)
    return [sample_from_dist(dist) for i in range(N)]

def sample_from_dist(dist):
    randnum = rnd.random()
    runningsum = 0
    for (value,probability) in dist.items():
        if runningsum <= randnum < (runningsum+probability):
            return value
        runningsum += probability
    return dist.keys()[-1]

def datagen(basis_yr,fname, year, month):
    yr = str(year)
    mon = str(month).zfill(2)
    yrmon = yr+mon # e.g. '202501'
    analysis_folder = '/'.join([base_folder,basis_yr,mon,'analysis'])

    df = pd.read_csv(analysis_folder + "/analysis_" + fname, index_col=0)
    cols = list(df['FieldName'])    
    outdata = pd.DataFrame(columns=cols)
    
    start_ts = int(df['Min']['time.1'])
    start_dt = dt.datetime.utcfromtimestamp(start_ts)
    
    #year  = start_dt.year
    #month = start_dt.month
    
    lastday = C_MonthLength[month]
    N = 24 * lastday      # We sample every day, once for each hour

    start_dt = dt.datetime(year, month, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    start_ts = int(dt.datetime.timestamp(start_dt))
    end_dt = dt.datetime(year, month, lastday, 23, 0, 0, tzinfo=dt.timezone.utc)
    end_ts = int(dt.datetime.timestamp(end_dt))

    for col in df['FieldName']:
        mu    = df['Mean'][col]
        sigma = df['StdDev'][col]

        dot = col.rfind('.') # Find where the last '.' is in the col-name
        if col[:dot].endswith("_qc"):  is_qc_field = True
        else:  is_qc_field = False
        
        if  (is_qc_field == True):   #Make QC type of data from its distribution
            outdata[col] = sampleN_data_dist(N, df['Distrib'][col])
        elif(is_qc_field == False): #Create a normally-distributed random list of values with mean=mu and StdDev=sigma:
            outdata[col] = pd.Series([rnd.normalvariate(mu,sigma) for i in range(N)])

    time_diff = 3600 # The number of seconds in 1 hour (60 x 60)
    times = [x for x in range(start_ts, end_ts+1, time_diff)] # Must add 1 to end_ts cos of how range() works
    datetimes = [dt.datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in times]

    outdata['datetime.1'] = pd.Series(datetimes)
    outdata['time.1']     = pd.Series(times)
    try: outdata['time_wpm_20.1'] = pd.Series(times)
    except: pass

    outdata.index = outdata['time.1']
    outdata.index.name = ''
    
    m = re.search("_[0-9]{6}", fname)  # find e.g: '_200507' in the fname
    output_name = fname.replace(m[0], "_"+yrmon) # Replace old date with new (yrmon)
    
    output_folder = base_folder+'/'+yr+'/'+mon+'/'+'csv'
    os.makedirs(output_folder, exist_ok=True)
    outdata.to_csv( output_folder + '/' + "sim_" + output_name)


if __name__ == "__main__":
    main()
