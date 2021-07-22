# -*- coding: utf-8 -*-
"""
Created on Thu May 27 21:45:49 2021
@author: Andrew Fox
"""

from netCDF4 import Dataset
import sys, os
import pandas as pd
import numpy as np
import random as rnd
import warnings
import yaml
from collections import OrderedDict, defaultdict
warnings.filterwarnings("ignore")
import datetime as dt


data_folder = "C:/Users/Barbf/Downloads/cmanwx"

MAX_FILES_TO_RUN = 2
C_year = '2021'


def main():
    (_,yeardirs,yearfiles) = next(os.walk(data_folder+'/'+C_year))

    for month in ['05']: # yeardirs:
        monthFolder = data_folder+'/'+C_year+'/'+month
        (_,_,files) = next(os.walk(monthFolder))
    
        for ds in files[:MAX_FILES_TO_RUN]:
            if ds.endswith(".nc"):
                convert_netCDF_file_to_csv(monthFolder, ds)


def convert_netCDF_file_to_csv(month_folder, ds):
    outfileName = ds.replace(".nc",".csv")
    x = Dataset(month_folder + '/' + ds, "r", format="NETCDF4")
    times = x['time'][:]
    colnames = ['base0.base1.datetime']
    fields_1 = OrderedDict();  fields_1['datetime.1'] = True
    fields_2 = OrderedDict();  fields_2['datetime.1'] = True
    
    for var in x.variables:
        vardata = x[var][:]
        colnames.append("base0.base1."+var)
        curr_var_num_try = 1
        while var+"."+str(curr_var_num_try) in fields_1:
            curr_var_num_try += 1
        fields_1[var+"."+str(curr_var_num_try)] = True

    for pload in x.groups.keys():
        for sensor in x[pload].groups.keys():
            sensor_data = x.createGroup(pload).createGroup(sensor)
            for attrib in sensor_data.variables.keys():
                colnames.append( pload+"."+sensor+"."+attrib )
                curr_attrib_num_try = 1
                while attrib+"."+str(curr_attrib_num_try) in fields_1:
                    curr_attrib_num_try += 1
                fields_1[attrib+"."+str(curr_attrib_num_try)] = True

    # Abort if NetCDF file has already been converted to csv:
    #if os.path.isfile(month_folder+"/csv/"+outfileName): return colnames

    # We now have the column/field names, so can now make the DataFrame:
    df = pd.DataFrame(columns = fields_1.keys(), index=times)
    df = df.fillna(0)
    df['datetime.1'] = [dt.datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in times]
    
    for var in x.variables:
        vardata = x[var][:]
        if type(vardata) == np.ma.core.MaskedArray:
            vardata = vardata.data.tolist()[0]
        try:
            curr_var_num_try = 1
            while var+"."+str(curr_var_num_try) in fields_2:
                curr_var_num_try += 1
            fields_2[var+"."+str(curr_var_num_try)] = True
            df[var+"."+str(curr_var_num_try)] = vardata
        except:
            print("Err_1: ", var, len(vardata))
            #if len(vardata)>=3750:
            #    print(vardata)
            #    df[var+"."+str(curr_var_num_try)] = pd.Series(vardata)
            pass

    df['time.1'] = times
    try: df['time_wpm_20.1'] = times
    except: pass

    for pload in x.groups.keys():
        for sensor in x[pload].groups.keys():
            sensor_data = x.createGroup(pload).createGroup(sensor)
            for attrib in sensor_data.variables.keys():
                try:
                    attrib_data = sensor_data[attrib][:]
                    curr_attrib_num_try = 1
                    while attrib+"."+str(curr_attrib_num_try) in fields_2:
                        curr_attrib_num_try += 1
                    fields_2[attrib+"."+str(curr_attrib_num_try)] = True
                    df[attrib+"."+str(curr_attrib_num_try)] = attrib_data
                except:
                    print("Err_2: ", attrib, curr_attrib_num_try)
                    print(attrib_data)
    
    # Save data to CSV file format in subdirectory 'csv/' in the data folder:
    outfileName = ds.replace(".nc",".csv")
    os.makedirs(month_folder+"/csv", exist_ok=True)
    df.to_csv(month_folder + "/csv/" + outfileName)
    return colnames



def analyse_fields_order(fields, exemplar_fields):
    exemplar_fields_order = OrderedDict(zip(exemplar_fields, range(1,1+len(exemplar_fields))))
    
    print('\n'+'\t'.join(['Dataset', '#Standard', '#MatchStandard', 'OrderDeviations',
                                 'AvgDeviation', '#Missing', '#NonStandard', '#Fields']))
    
    for ds in fields:
        ds_fields = fields[ds]
        ds_fields_order = OrderedDict(zip(ds_fields, range(1,1+len(ds_fields))))
        ds_total_deviation = 0
        num_matching = 0
        ex_fi_notIn_ds = 0
        ds_fi_nonex = set(ds_fields).difference(set(exemplar_fields))

        for field in exemplar_fields_order:
            ex_fi_order = exemplar_fields_order[field]
            try:
                ds_fi_order = ds_fields_order[field]
                curr_deviation = abs(ex_fi_order-ds_fi_order)
                num_matching += 1
                ds_total_deviation += curr_deviation
            except:
                ex_fi_notIn_ds += 1
        
        ds_dev_per_match = round(ds_total_deviation/num_matching, 1)

        print('\t'.join(map(str,[ds, len(exemplar_fields), num_matching, ds_total_deviation,
                                 ds_dev_per_match, ex_fi_notIn_ds, len(ds_fi_nonex), len(ds_fields)])))


def analyse_fields(fields, exemplar_fields, is_exemplar):
    new_fields = OrderedDict()
    new_counts = OrderedDict()
    # ds stands for dataset(name)
    for ds in fields:
        ds_fields = fields[ds]
        new_fields[ds] = OrderedDict()
        new_counts[ds] = defaultdict(int)
        for field in ds_fields:
            varname = field.split(".")[-1]
            curr_varnum_try = 1
            while varname+"."+str(curr_varnum_try) in new_fields[ds]:
                curr_varnum_try += 1
            new_fields[ds][varname+"."+str(curr_varnum_try)] = 1
            new_counts[ds][varname] += 1
    exemplar_fields_set = set(exemplar_fields)
    
    if not is_exemplar:
        print('\n'+'\t'.join(['Dataset', '#Fields', '#Standard', '#MatchStandard', '%Matched']))
    for ds in new_fields:  # ds stands for dataset(name)
        ds_fields_set = set(new_fields[ds])
        num_desired   = len(exemplar_fields_set.intersection(ds_fields_set))
        perc_desired  = round(100*num_desired/len(exemplar_fields_set), 1)
        if not is_exemplar:
            print( '\t'.join([ds, str(len(new_fields[ds])), str(len(exemplar_fields_set)), str(num_desired), str(perc_desired)+"%"]))
    return new_fields, new_counts


if __name__ == "__main__":
    # run main function:
    main()
