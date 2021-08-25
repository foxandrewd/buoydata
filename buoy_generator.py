"""
Created on: August 13, 2021
@author: Andrew Fox
"""

import os, sys, yaml
import pandas as pd
import numpy as np
import random as rnd
import datetime as dt
import re
import warnings
warnings.filterwarnings("ignore")
from multiprocessing import Pool

'''
base_folder = 'C:/Users/Barbf/Downloads/cmanwx'
BASIS_YEAR = '2020'
MAX_FILES_TO_RUN = 4
CSV_FOLDER = 'csv'
SIM_PREFIX = 'simdata'
OUTPUT_YEARS = [2022]
OUTPUT_MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]
DATA_FREQ_IN_HOURS = 2
'''

#global base_folder, BASIS_YEAR, MAX_FILES_TO_RUN, CSV_FOLDER
#global SIM_PREFIX, OUTPUT_YEARS, OUTPUT_MONTHS, DATA_FREQ_IN_HOURS


# Zero'th month doesn't exist:
C_MonthLength = [0,31,28,31,30,31,30,31,31,30,31,30,31]


def extract_buoy_names(fnames):
    buoy_fnames = {}
    for fname in fnames:
        if '_adcp_' in fname: continue   # these '_adcp_' files have almost no data
        buoy = fname_to_buoyname(fname)
        if buoy not in buoy_fnames:
            buoy_fnames[buoy] = fname  # Keep buoy's 1st file, ignore it's 2nd, 3rd,..
        else: continue
    return buoy_fnames

def pool_initializer(params):
    global base_folder, BASIS_YEAR, MAX_FILES_TO_RUN, CSV_FOLDER
    global SIM_PREFIX, OUTPUT_YEARS, OUTPUT_MONTHS, DATA_FREQ_IN_HOURS

    OUTPUT_YEARS       = params['outyears']
    OUTPUT_MONTHS      = params['outmonths']
    BASIS_YEAR         = params['basisyear']
    base_folder        = params['basisfolder']
    MAX_FILES_TO_RUN   = params['numbuoys']
    CSV_FOLDER         = params['csvfolder']
    SIM_PREFIX         = params['simprefix']
    DATA_FREQ_IN_HOURS = params['datafreqinhours']

def main(params):
    
    global base_folder, BASIS_YEAR, MAX_FILES_TO_RUN, CSV_FOLDER
    global SIM_PREFIX, OUTPUT_YEARS, OUTPUT_MONTHS, DATA_FREQ_IN_HOURS

    OUTPUT_YEARS       = params['outyears']
    OUTPUT_MONTHS      = params['outmonths']
    BASIS_YEAR         = params['basisyear']
    base_folder        = params['basisfolder']
    MAX_FILES_TO_RUN   = params['numbuoys']
    CSV_FOLDER         = params['csvfolder']
    SIM_PREFIX         = params['simprefix']
    DATA_FREQ_IN_HOURS = params['datafreqinhours']

    print('\n 0 base_folder:', base_folder)
    print(' 0 MAX_FILES_TO_RUN:', MAX_FILES_TO_RUN)
    print(' 0 OUTPUT_YEARS:', OUTPUT_YEARS)
    print(' 0 OUTPUT_MONTHS:', OUTPUT_MONTHS)
    print(' 0 SIM_PREFIX:', SIM_PREFIX)
    print(' 0 DATA_FREQ_IN_HOURS:', DATA_FREQ_IN_HOURS)

    analysis_arguments = []
    datagen_arguments = []    

    for month in OUTPUT_MONTHS:
        mon = str(month).zfill(2)
        try: (_,_,files) = next(os.walk('/'.join([base_folder, BASIS_YEAR, mon, CSV_FOLDER])))
        except: continue
        buoyfiles = list(extract_buoy_names(files).values())
        for fname in buoyfiles[:MAX_FILES_TO_RUN]:
            ### run_analysis(BASIS_YEAR, fname, BASIS_YEAR, month)   # Year is always BASIS_YEAR for basis analysis
            analysis_arguments.append( (BASIS_YEAR, fname, BASIS_YEAR, month) )

    pool_1 = Pool( processes=os.cpu_count(), 
                   initializer=pool_initializer, 
                   initargs=(params,) )
    
    pool_1.starmap(run_analysis, analysis_arguments)
    
    # Generate simulated data for all output years and months:
    for year in OUTPUT_YEARS:
        for month in OUTPUT_MONTHS:
            mon = str(month).zfill(2)
            try: (_,_,files) = next(os.walk('/'.join([base_folder, BASIS_YEAR, mon, CSV_FOLDER])))                    
            except: continue            
            buoyfiles = list(extract_buoy_names(files).values())
            for fname in buoyfiles[:MAX_FILES_TO_RUN]:
                ### datagen(BASIS_YEAR, fname, year, month)
                datagen_arguments.append( (BASIS_YEAR, fname, year, month) )

    pool_2 = Pool( processes=os.cpu_count(), 
                  initializer=pool_initializer, 
                  initargs=(params,) )

    pool_2.starmap( datagen , datagen_arguments )


    # Open File Explorer to show the final year of data generated:
    final_yr_dir = base_folder+"/"+str(year)
    final_yr_dir_bslash = final_yr_dir.replace("/", "\\")
    final_yr_dir_raw = r'{}'.format(final_yr_dir_bslash)
    os.system(r'explorer ' + final_yr_dir_raw)


def run_analysis(basis_yr, fname, year, month):

    global base_folder, BASIS_YEAR, MAX_FILES_TO_RUN, CSV_FOLDER
    global SIM_PREFIX, OUTPUT_YEARS, OUTPUT_MONTHS, DATA_FREQ_IN_HOURS

    yr = str(year)
    mon = str(month).zfill(2)
    yrmon = yr+mon # e.g. '202501'

    analysis_folder = '/'.join([base_folder, basis_yr, mon, 'analysis'])
    analysis_fname = analysis_folder + '/' + "analysis_" + fname

    if os.path.isfile(analysis_fname):
        print('Analysis already exists:', yr, mon, ", Buoy:", fname_to_buoyname(fname))
        return
    else:
        print('Analysing:', yr, mon, ", Buoy:", fname_to_buoyname(fname))

    print('\n 1 base_folder:', base_folder)
    print(' 1 MAX_FILES_TO_RUN:', MAX_FILES_TO_RUN)
    print(' 1 OUTPUT_YEARS:', OUTPUT_YEARS)
    print(' 1 OUTPUT_MONTHS:', OUTPUT_MONTHS)
    print(' 1 SIM_PREFIX:', SIM_PREFIX)
    print(' 1 DATA_FREQ_IN_HOURS:', DATA_FREQ_IN_HOURS)

    
    df = pd.read_csv('/'.join([base_folder, yr, mon, CSV_FOLDER, fname]), index_col=0)
    outcolumns = ["FieldName","DataType","Min","Max","Mean","StdDev","Median","Mode",
                  "NumValues","NumNulls","NumUnique","AutoCorr","FFT","Distrib"]
    dfout = pd.DataFrame(index=df.columns, columns = outcolumns)
    time_gap_hours = (df.index[1] - df.index[0])/3600.0

    for col in df:
        try: df[col] = df[col].astype('float64')
        except: pass    
        if 'float' in str(df[col].dtype) and is_integer_column(df[col]):
            try: df[col] = df[col].fillna(-32767).astype('int64')
            except ValueError: pass
        if df[col].dtype in ('int64','float64'):
            df[col][df[col] > 1e34] = np.nan
            df[col][df[col] < -1e6] = np.nan
        if 'int' in str(df[col].dtype):
            df[col][df[col] == -32767] = np.nan
    
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

def is_integer_column(col):
    for num in list(col):
        if not num.is_integer(): # Find a non-int, just return False
            return False
    return True  

def sampleN_data_dist(N, dist):
    dist = yaml.load(dist)
    return [sample_from_dist(dist) for i in range(N)]

def sample_from_dist(dist):
    randnum = rnd.random()
    runningsum = 0
    for (value,probability) in dist.items():
        if runningsum <= randnum < (runningsum+probability):
            return value
        runningsum += probability
    return dist.keys()[-1]

def fname_to_buoyname(fname):
    newname = fname
    if newname.startswith('simdata_'): # Remove 'simdata_' if at the start of fname
        newname = newname[8:]
    if newname.startswith('sim_'):
        newname = newname[4:]      # Remove 'sim_' from the start of the name
    if newname.startswith('NDBC_'):
        newname = newname[5:]      # Remove 'NDBC_' from the start of the name    
    uscore = newname.index('_')    # Position of the '_' char in newname
    buoyname = newname[:uscore]    # Buoyname is before the '_' char
    return buoyname

def datagen(basis_yr, fname, year, month):

    global base_folder, BASIS_YEAR, MAX_FILES_TO_RUN, CSV_FOLDER
    global SIM_PREFIX, OUTPUT_YEARS, OUTPUT_MONTHS, DATA_FREQ_IN_HOURS

    yr = str(year)
    mon = str(month).zfill(2)
    yrmon = yr+mon # e.g. '202501'
    analysis_folder = '/'.join([base_folder,basis_yr,mon,'analysis'])
    
    df = pd.read_csv(analysis_folder + "/analysis_" + fname, index_col=0)
    cols = list(df['FieldName'])
    outdata = pd.DataFrame(columns=cols)
    
    start_ts = int(df['Min']['time.1'])
    start_dt = dt.datetime.utcfromtimestamp(start_ts)
    
    lastday = C_MonthLength[month]
    N = int(24 * lastday / DATA_FREQ_IN_HOURS)      # We sample every day, once for each hour, div by num hours diff to sample by

    start_dt = dt.datetime(year, month, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
    start_ts = int(dt.datetime.timestamp(start_dt))
    
    end_dt = dt.datetime(year, month, lastday, 23, 59, 58, tzinfo=dt.timezone.utc)
    end_ts = int(dt.datetime.timestamp(end_dt))

    for col in df['FieldName']:
        # Get the mean and standard deviation for this field:
        mu    = df['Mean'][col]
        sigma = df['StdDev'][col]
        dot = col.rfind('.') # Find where the last '.' is in the col-name
        if col[:dot].endswith("_qc"):  is_qc_field = True
        else:                          is_qc_field = False
        
        if is_qc_field: #Make QC type of data from its distribution
            outdata[col] = pd.Series(sampleN_data_dist(N, df['Distrib'][col]))
        else:        #Create a normally-distributed random list of values with mean=mu and StdDev=sigma:
            outdata[col] = pd.Series([rnd.normalvariate(mu,sigma) for i in range(N)])

    time_diff = 3600 # The number of seconds in 1 hour (60 x 60)
    time_diff *= DATA_FREQ_IN_HOURS   # If freq is 2hrs then diff is 3600*2 = 7200 seconds
    times = [x for x in range(start_ts, end_ts+1, time_diff)] # Add 1 to end_ts cos of how range() works
    datetimes = [dt.datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S') for t in times]
    
    # Make the date/time fields
    outdata['datetime.1'] = pd.Series(datetimes)
    outdata['time.1']     = pd.Series(times)
    try: outdata['time_wpm_20.1'] = pd.Series(times)
    except: pass

    print('\n base_folder:', base_folder)
    print(' MAX_FILES_TO_RUN:', MAX_FILES_TO_RUN)
    print(' OUTPUT_YEARS:', OUTPUT_YEARS)
    print(' OUTPUT_MONTHS:', OUTPUT_MONTHS)
    print(' SIM_PREFIX:', SIM_PREFIX)
    print(' DATA_FREQ_IN_HOURS:', DATA_FREQ_IN_HOURS)


    buoyname = fname_to_buoyname(fname)
    outdata.index = outdata['time.1']
    outdata.index.name = buoyname
    
    output_fname = '_'.join([SIM_PREFIX, buoyname, yrmon ]) + ".csv"
    output_folder = '/'.join([base_folder, yr, mon, CSV_FOLDER])
    os.makedirs(output_folder, exist_ok=True)
    outdata.to_csv( output_folder + '/' + output_fname)

    print('Generating year-month: ', yr + " - " + mon, ', Buoy:', buoyname)


if __name__ == "__main__":
    main(None)
