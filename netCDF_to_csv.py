# -*- coding: utf-8 -*-
"""
Created on Thu May 27 21:45:49 2021
@author: Andrew Fox
"""

from netCDF4 import Dataset
from netCDF4 import num2date
import sys, os
import pandas as pd
import warnings
from collections import OrderedDict, defaultdict
warnings.filterwarnings("ignore")

data_folder = "B:\\ftp\\nodc.noaa"
data_folder = data_folder.replace( "\\", "/" ) # change any backslashes to forwardslashes
exemplar = "NDBC_41001_202101_D4_v00.nc" # File to use as the 'standard' format file
MAX_FILES_TO_RUN = 200

def main():
    for root,_,files in os.walk(".", topdown=False):
        for name in files:
            #print(os.path.join(root, name))
        #print(files)
            #_,_,files = next(os.walk(data_folder,topdown=False))
            #print(files,os.walk(data_folder,topdown=False))
            fields = {}
            exemplar_fields_raw = {}
            exemplar_fields_raw[exemplar] = convert_netCDF_file_to_csv(data_folder, exemplar)
            exemplar_fields = analyse_fields(exemplar_fields_raw, exemplar_fields_raw, True)[0][exemplar]
            t_count=[0,0]

            for ds in files:#[:MAX_FILES_TO_RUN]:
                t_count[0]+=1
                if ds.endswith(".nc"):
                    t_count[1]+=1
                    ds=root+"/"+ds
                    ds=ds.replace("\\","/")
                    ds=ds.replace(".nc","~nc")
                    ds=ds.replace(".","")
                    ds=ds.replace("~nc",".nc")
                    print(files,ds)
                    curr_fields = convert_netCDF_file_to_csv(data_folder, ds)
                    fields[ds] = curr_fields[:]
                
            new_fields,new_counts = analyse_fields(fields, exemplar_fields, False)
            print(fields,t_count)
            analyse_fields_order(new_fields, exemplar_fields)
    input("press any key to continue...")

def analyse_fields_order(fields, exemplar_fields):
    exemplar_fields_order = OrderedDict(zip(exemplar_fields, range(1,1+len(exemplar_fields))))
    
    print('\t'.join(['Dataset', '#Standard', '#MatchStandard', 'OrderDeviations',
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
        print('\t'.join(['Dataset', '#Fields', '#Standard', '%Standard']))
    for ds in new_fields:  # ds stands for dataset(name)
        ds_fields_set = set(new_fields[ds])
        num_desired   = len(exemplar_fields_set.intersection(ds_fields_set))
        perc_desired  = round(100*num_desired/len(exemplar_fields_set), 1)
        if not is_exemplar:
            print( '\t'.join([ds, str(len(new_fields[ds])) , str(num_desired), str(perc_desired)+"%"]))
    return new_fields, new_counts

def convert_netCDF_file_to_csv(data_folder, ds):
    print(data_folder,ds)
    y=data_folder + "/" + ds
    y=y.replace("//","/")
    x = Dataset(y, "r", format="NETCDF4")
    times = x['time'][:]
    colnames = ['base0.base1.datetime']
    fields_1 = OrderedDict();  fields_1['datetime.1'] = 1;
    fields_2 = OrderedDict();  fields_2['datetime.1'] = 1;
    
    for var in x.variables:
        vardata = x[var][:]
        if len(vardata) == len(times): # Add in if variable has the same length as 'times'
            colnames.append("base0.base1."+var)
            fields_1[var+".1"] = 1
            
    for pload in x.groups.keys():
        for sensor in x[pload].groups.keys():
            sensor_data = x.createGroup(pload).createGroup(sensor)
            for attrib in sensor_data.variables.keys():
                colnames.append( pload+"."+sensor+"."+attrib )
                curr_attrib_num_try = 1
                while attrib+"."+str(curr_attrib_num_try) in fields_1:
                    curr_attrib_num_try += 1
                fields_1[attrib+"."+str(curr_attrib_num_try)] = 1
    
    # Now have the column/field names, so can now make the DataFrame:
    df = pd.DataFrame(columns = fields_1.keys(), index=times)
    df = df.fillna(0)
    df['datetime.1'] = num2date(times, x['time'].units)
    
    for var in x.variables:
        vardata = x[var][:]
        if len(vardata) == len(times):
            df[var + ".1"] = vardata
    
    for pload in x.groups.keys():
        for sensor in x[pload].groups.keys():
            sensor_data = x.createGroup(pload).createGroup(sensor)
            for attrib in sensor_data.variables.keys():
                #try: df[pload + "." + sensor + "." + attrib] = sensor_data[attrib][:]
                #except: pass
                try:
                    curr_attrib_num_try = 1
                    while attrib+"."+str(curr_attrib_num_try) in fields_2:
                        curr_attrib_num_try += 1
                    fields_2[attrib+"."+str(curr_attrib_num_try)] = 1
                    df[attrib+"."+str(curr_attrib_num_try)] = sensor_data[attrib][:]
                except: pass
    
    # Save data to CSV file format in subdirectory 'csv/' in the data folder:
    dh=ds.replace("/","-")
    outfileName = dh.replace(".nc",".csv")
    if not os.path.isdir(data_folder+"/csv"): os.mkdir(data_folder+"/csv")
    yf=data_folder + "/csv/" + outfileName
    yf=yf.replace("//","/")
    df.to_csv(yf)
    
    return colnames

if __name__ == "__main__":
   main()