from __future__ import print_function
 
from datetime import datetime as dt
#import statistics as stats
import numpy as np
import pandas as pd
import csv, os, json
from IPython.core.display import HTML
#import tkinter as tk
#from tkinter import filedialog
#from tkinter import messagebox as mb
import re
import subprocess
import collections, sys

import os
memory="10g"
pyspark_submit_args = ' --executor-memory ' + memory + ' --driver-memory ' + memory + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
 
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

#spark = SparkSession.builder.appName("PySparkSQL").config("spark.kryoserializer.buffer.max", "1024m").getOrCreate()
#spark.sql("set spark.sql.caseSensitive=true")


"""
Table of Contents/Navigation
(Search a hashed tag to skip to a section in this document)
 
 
Dip Code                          "#[DIP]"
Data Identity Class               "#[DID]"
General Data Profiling Functions  "#[DPF]"
Dataframe/Column Functions        "#[DFC]"
Andrew Fox's Data Functions       "#[AFOX]"
Data Identity Builder             "#[DID_BUILD]"
Data Configuration                "#[DAT_CONFIG]"
Data Identifier                   "#[DAT_IDENT]"
Data Profiler                     "#[DAT_PROF]"
Unknown Data Profiler             "#[DAT_UPROF]"
"""
 
### Imports for Data Flattening ####################################
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructType, ArrayType
from pyspark.sql import DataFrame
import re
#############################################################

 
####################################################################
### Data Flattening Functions:
 
def __rename_nested_field__(in_field, fieldname_normaliser):
    if isinstance(in_field, ArrayType):
        dtype = ArrayType(__rename_nested_field__(in_field.elementType, fieldname_normaliser), in_field.containsNull)
    elif isinstance(in_field, StructType):
        dtype = StructType()
        for field in in_field.fields:
            dtype.add(fieldname_normaliser(field.name), __rename_nested_field__(field.dataType, fieldname_normaliser))
    else:
        dtype = in_field
    return dtype
 
def __normalise_fieldname__(raw):
    return re.sub('[^A-Za-z0-9_]+', '_', raw.strip())
 
def __get_fields_info__(dtype, name = ""):
    ret = []
    if isinstance(dtype, StructType):
        for field in dtype.fields:
            for child in __get_fields_info__(field.dataType, field.name):
                wrapped_child = ["{prefix}{suffix}".format(
                    prefix=("" if name == "" else "`{}`.".format(name)), suffix=child[0])] + child[1:]
                ret.append(wrapped_child)
    elif isinstance(dtype, ArrayType) and (
            isinstance(dtype.elementType, ArrayType) or isinstance(dtype.elementType, StructType)):
        for child in __get_fields_info__(dtype.elementType):
            wrapped_child = ["`{}`".format(name)] + child
            ret.append(wrapped_child)
    else:
        return [["`{}`".format(name)]]
    return ret
 
def normalise_fields_names(df, fieldname_normaliser=__normalise_fieldname__):
    return df.select([
        F.col("`{}`".format(field.name)).cast(__rename_nested_field__(field.dataType, fieldname_normaliser))
            .alias(fieldname_normaliser(field.name)) for field in df.schema.fields
    ])
 
""" This is the KEY Function here, flatten_spark_dataframe(sparkDF) """
def flatten_spark_dataframe(sparkDF, fieldname_normaliser=__normalise_fieldname__):
    cols = []
    for child in __get_fields_info__(sparkDF.schema):
        if len(child) > 2:
            ex = "x.{}".format(child[-1])
            for seg in child[-2:0:-1]:
                if seg != '``':
                    ex = "transform(x.{outer}, x -> {inner})".format(outer=seg, inner=ex)
            ex = "transform({outer}, x -> {inner})".format(outer=child[0], inner=ex)
        else:
            ex = ".".join(child)
        cols.append(F.expr(ex).alias(fieldname_normaliser("/".join(child).replace('`', ''))))
    return sparkDF.select(cols)
 
### End SparkAid functions
####################################################################
 
 
 
class My_StringVar:
  def __init__(self):
    self.theString = ""
  def get(self):
    return self.theString
  def set(self, newString):
    self.theString = newString
 
def convert_all_to_strings(myseries):
    i = -1
    for val in myseries.iteritems():
        i += 1
        try:
            myseries.loc[i] = str(val)
            print("yess: ")
            print(val)           
        except:
            print("fail: ")
            print(val)
            myseries.loc[i] = ""
    print("done:")
    return myseries
 
def is_map_column(df, col):
  colvals = list(df[col])
  total=0.0
  maps=0.0
  for val in colvals[:3000]:
      if val is None: continue
      else: total += 1.0
      str_val = str(val)
      startStr = str_val[:4]
      endStr = str_val[-1:]
      if startStr=='Row(' and endStr==')' and """=u'""" in str_val:
          maps += 1.0
      #endif
  #endfor
  try:
      proportion_maps = float(maps)/float(total)
  except ZeroDivisionError:
      proportion_maps = 0.0
  if proportion_maps > 2/3.0:    # 2 out of 3 ain't bad...
      is_a_map_col = True
  else:
      is_a_map_col = False
  return is_a_map_col
 
 
def extract_map_col(df, col):
    colvals = list(df[col])
    m = collections.OrderedDict()
    for val in colvals:
        if val is None:
            # Add a whole blank line to output 'm' dict
            for k in m: m[k].append(None)
        else:
            line = str(val)[4:-1]
            kvpairs = line.strip().split(", ")
            (colnames, values) = get_keys_and_values(kvpairs)
            for (j,colname) in enumerate(colnames):
                if colname not in m: m[colname] = []
                m[colname].append(values[j])
    map_df = pd.DataFrame.from_dict(m)
    return map_df
     
def get_keys_and_values(kvpairs):
    keys=[]
    values=[]
    for kv0 in kvpairs:
        kv = kv0.strip()
        try:
          [key,value0] = kv.split("=u")
        except:
          print(kv)
          sys.exit()
        value = value0.strip("'")
        keys.append(key)
        values.append(value)
         
    return (keys, values)
   
def extract_structs_from_df(df):
  structs = collections.OrderedDict()
  for col in df.columns:
    col_is_map = is_map_column(df, col)
    print(col + " is a map?: " + str(col_is_map))
    if col_is_map:
      map_df = extract_map_col(df, col)
      structs[col] = map_df
  return structs
 
 
def df_replace_all_the_nulls(df):
  for field_name in df.columns:
    df.loc[ pd.isnull(df[field_name]) ] = None
  return df
 
 
class MetaFrame:
    def __init__(self,master=None):
        #tk.Frame.__init__(self)
         
        self.file = My_StringVar()
        self.df = None
 
        '''
        # On CDSW platform, we don't do GUI
        fileButton = tk.Button(self, text='Select file', width=10, command=self.fdialog)
        fileButton.grid(row=0,column=0,padx=5,pady=12)
        fLabel = tk.Label(self,textvariable=self.file,bg='white',relief='sunken',width=30,anchor='w')
        fLabel.grid(row=0,column=1,padx=(0,5),pady=12)
        frame=tk.Frame(self)
        goButton = tk.Button(frame,text='Generate metadata',width=15,command=self.gendata)
        goButton.grid(row=0,column=1,pady=5,padx=10)
        quitButton = tk.Button(frame,text='Close',width=15,command=master.destroy)
        quitButton.grid(row=0,column=2,pady=5,padx=10)
        frame.grid(row=1,column=0,columnspan=2)
        frame.columnconfigure(0,weight=1)
        frame.columnconfigure(3,weight=1)
        self.columnconfigure(0,weight=1)
        self.pack()
        '''
         
    def unique_count(self,list_name,value):
        x = 0
        for r in list_name:
            try:
                if r.find(value) != -1:
                    x += 1
            except:
                pass
        return x
 
 
    def fdialog(self):
        self.file.set(filedialog.askopenfilename(initialdir =os.getcwd(),title = "Select file",filetypes = (("csv files","*.csv"),("all files","*.*"))))
     
    # Method to run the METADATA Analysis:
    def generate_full_metadata(self, df, dataSourceName, dayDesired, numRecsDesired, num_records_string, dataType):
 
                length = df.shape[0]       
                try:    dupes = df.duplicated().sum()
                except: dupes = 0
                output = pd.DataFrame(index=pd.Index(df.columns,name='Field Name'),columns=['Field_Name', 'Data Type','Data Usage','% Null','Nullable','Array Size','Categorical/Continuous','Max Character Length','Notes'])
 
                try:
                    df_factored = df.apply(lambda x : pd.factorize(x)[0] )
                    corrmatrix = df_factored.corr(method='pearson', min_periods=1)
                except:
                    corrmatrix = None
 
                # MAIN Metadata FOR LOOP:
                print("Running Metadata Analysis...")
                for col in df.columns:
                    output.loc[col,'Field_Name'] = col
                    print( "Metadata: Now processing Column: " + str(col) )
           
                    colList = list(df[col])
           
                    ###print("ColList is: " + str(colList))
           
                    try:
                        col_percent_unique = df[col].nunique()/float(df[col].count())
                    except (TypeError, ZeroDivisionError):
                        col_percent_unique = None
                    if col_percent_unique == None:
                        output.loc[col,'Data Usage'] = "Category"
                    elif col_percent_unique < 0.8:
                        output.loc[col,'Data Usage'] = "Category"
                    else:
                        if df[col].dtype == 'object':
                            if df[col].dropna().str.contains(" ").sum() > 0:
                                output.loc[col,'Data Usage'] = "Description"
                            else:
                                output.loc[col,'Data Usage'] = "ID"
                        elif df[col].dtype in ('int64','float64'):
                            output.loc[col,'Data Usage'] = "ID"
                        else:
                            output.loc[col,'Data Usage'] = "Description"
                     
                    nulls = countNullValues(list(df[col]))[0]
                     
                    output.loc[col,'% Null'] = 100*(nulls/float(length))
                     
                    if output.loc[col,'% Null'] > 0:
                        output.loc[col,'Nullable'] = True
                    else:
                        output.loc[col,'Nullable'] = False
                     
                    if df[col].dtype in ('int64','float64','datetime64[ns]'):
                         
                        if (df[col].dtype == 'float64') and df[col].equals(df[col].round()):
                            try:
                                output.loc[col,'Array Size'] = str(int(df[col].max()-df[col].min()))
                            except:
                                output.loc[col,'Array Size'] = "N/A"
                        else:
                            try:
                                output.loc[col,'Array Size'] = str(df[col].max()-df[col].min())
                            except:
                                output.loc[col,'Array Size'] = "N/A"
                    else:
                        output.loc[col,'Array Size'] = "N/A"
                    if df[col].dtype in ('int64','int32','float64','float32','datetime64[ns]') :
                        output.loc[col,'Categorical/Continuous'] = "Continuous"
                    else:
                        output.loc[col,'Categorical/Continuous'] = "Categorical"
                     
                    try:
                        df_col_num_unique = df[col].nunique()
                    except TypeError:
                        df_col_num_unique = None
                         
                    if df_col_num_unique == 1:
                        #print()
                        #print(  str(df[col].dropna())  )
                        note = "All values identical ('" + str(list(df[col].dropna())[0]) + "')"
                        #print("col is: " + col)
                        #print("note is: " + note)
                    else:
                        note = ""
                        dupe = []
                        redun = []
                        for col2 in df.columns.drop(col):
                            if df[col].dtype == df[col2].dtype:
                                if (df[col] == df[col2]).all():
                                    dupe.append(col2)
                            else:
                              try:
                                col2_is_redundant = df[[col2,col]].apply(lambda x: str(x[1]) in str(x[0]),axis=1).all()
                              except:
                                col2_is_redundant = False
                              if col2_is_redundant:
                                redun.append(col2)
                        if len(dupe) > 0:
                            note = note + "Column is a duplicate of column/s " + "; ".join(dupe) + "; "
                        if len(redun) > 0:
                            note = note + "Possible redundancy - all values contained within column/s " + "; ".join(redun) + "; "
                         
                        try:
                            col_percent_unique = df[col].nunique()/float(df[col].count())
                        except (TypeError, ZeroDivisionError):
                            col_percent_unique = None
                        if col_percent_unique == None:
                          output.loc[col,'Data Usage'] = "Category"
                        elif col_percent_unique < 0.8:
                          if corrmatrix is not None:
                                for (lbl,val) in corrmatrix[col].drop(col).iteritems():  ## PYTHON2_CHANGE
                                    if val > 0.9:
                                        note = note + str(int(val*100)) + "% correlation with " + lbl + "; "
                        if len(note) > 0:
                            note = note[:-1]   
                    output.loc[col,'Notes'] = note
                    #data type recognition comes after other methods, as we will potentially modify some column data
                    if df[col].dtype == 'object':
                        try:
                            stringlen = df[col].str.strip().str.len()
                        except:
                            stringlen = None
                        if stringlen is not None:
                            if stringlen.min() == stringlen.max():
                                output.loc[col,'Data Type'] = "Fixed String(" + str(int(stringlen.max())) + ")"
                            else:
                                try:
                                    output.loc[col,'Data Type'] = "Var String(" + str(int(stringlen.max())) + ")"
                                except:
                                    output.loc[col,'Data Type'] = "Var String"
                        else:
                            output.loc[col,'Data Type'] = "Var String"
                    elif df[col].dtype == 'datetime64[ns]':
                        output.loc[col,'Data Type'] = "Datetime"
                    elif df[col].dtype == 'int64':
                        if df[col].min() < 0:
                            output.loc[col,'Data Type'] = "Mixed Int"
                        else:
                            output.loc[col,'Data Type'] = "Positive Int"
                    elif df[col].dtype == 'float64':
                        if df[col].equals(df[col].round()):   # True only if col contains ALL integers!!
                            
                            try: df[col] = df[col].astype('object').apply(lambda x: int(x) if x == x else "")
                            except: df[col] = df[col].astype('int')
                            #NaN values are float, so a numerical column with nulls will be treated as a float
                            #so if all non-null values are ints, we convert the column type to 'object', change
                            #the non-null values to ints, and the null values to a blank string
                            if df[col].replace("",0).min() < 0:
                                output.loc[col,'Data Type'] = "Mixed Int"
                            else:
                                output.loc[col,'Data Type'] = "Positive Int"
                        elif df[col].min() < 0.:
                            output.loc[col,'Data Type'] = "Mixed Float"
                        else:
                            output.loc[col,'Data Type'] = "Positive Float"
                    else:
                        output.loc[col,'Data Type'] = df[col].dtype
                     
                    #Aaron's Logic
                    #character length
                    try:
                        # stringlen = df[col].str.strip().str.len()   # Maxlength of string from Data Type (above)
                        # For consistency add in .str.strip() to the line below:
                        output.loc[col,'Max Character Length'] = (set(list(df[col].str.strip().str.len())).values())
                    except:
                        count_1 = []
                        for i in list(df[col]):
                            try:
                                count_1.append(len(str(i).strip()))     # Added in strip fn to remove whitespace
                            except:
                                pass
                        output.loc[col,'Max Character Length'] = max(count_1)
                #endforloop   
 
                # MAIN MetaData FOR LOOP IS FINISHED
                #======================================#
                                 
                if self.file.get().rfind('.') != -1:
                    fname = self.file.get()[:self.file.get().rfind('.')]
                                
                else:
                    fname = self.file.get()
                     
                path = os.path.dirname(fname)
                path = path + '/'
                ffname = re.sub(path,'',fname)
                fffname = ffname+".txt"
                fname = fname + " metadata.html"
                 
                pd.set_option('display.max_colwidth', 500)
           
                html = output.to_html(index_names=False, index=False)
                 
                print("self.file.get() is: " + self.file.get())

                #print("ffname is: " + ffname)
                #print("fffname is: " + fffname)
                #print("fname is: " + fname)
                 
                if self.file.get().rfind('/') != -1:
                    ftitle = self.file.get()[self.file.get().rfind('/')+1:]
                else:
                    ftitle = self.file.get()
                code = """<html>
                    <h3>""" + ftitle + """ Metadata</h3>
                    <body><p>Record Count : """ + str(length) + """</p><p>Duplicate Record Count: """ + str(dupes) + """</p></body>
                    """
                code = code + html + """</html>"""
 
                #fname = "Profiling/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+numRecsDesired+"_"+dataType+"_"+num_records_string+"_metadata.html"
                
                fname = "../outdata/metadata/metadata_" + self.inputname + "_" + str(self.CONST_LIMIT_RECORDS) + ".html"
                print("fname is: " + fname)
                outfile = open(fname,'w')
                outfile.write(code)
                outfile.close()
                 
                ####mb.showinfo("","Metadata successfully generated and saved as " + fname)
 
                # Change all metadata output column datatypes to string, so it can be written out as a CSV file
                for outCol in output.columns:
                  #print("OutCol is: " + outCol + ", dtype is: " + str(output[outCol].dtype))
                  output[outCol] = output[outCol].astype(str)
                 
                #output_schema = ['Field_Name','Data Type','Data Usage','% Null','Nullable','Array Size','Categorical/Continuous','Max Character Length','Notes']
                #output_SparkDF = spark.createDataFrame(output, output_schema)
                #print("Created output_SparkDF successfully.")
                 
                # Write the METADATA Output into one (1) single file:
                #output_SparkDF.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep="|").save(path="hdfs:///product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/output_"+dataSourceName+"_"+dayDesired+"_"+dataType+"_"+num_records_string+"_metadata.txt")
                output.to_csv("../outdata/metadata/metadata_" + self.inputname + "_" + str(self.CONST_LIMIT_RECORDS) + ".csv")
 
    def gendata(self):
        dataSourceName = "NDBC" # or "cardex"
        dayDesired = "20201201" # 20201005 to 20201009 for Cardex/SCOM 
        numRecsDesired = "2500"  # 0 means unlimited num. of records
        # scom has 25000 records each, cardex has 50000 each.
  
        num_records_string ="10"    # 0 means no limit on num. of records
        self.CONST_LIMIT_RECORDS = int(num_records_string)
        
        self.inputname = "NDBC_41001_202101_D4_v00"

        #print(self.inputname)

        #self.file.set("../../product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+numRecsDesired+".parquet")
        self.file.set( "../indata/csv/"+ self.inputname + ".csv")
        
        print("dataSourceName: " + dataSourceName)
        #df_0 = spark.read.parquet( self.file.get() )
        df_0 = pd.read_csv( self.file.get() )
        df = df_0.head(self.CONST_LIMIT_RECORDS)
        
        """
        ## Try to flatten this PySpark DataFrame (df_0):
        if self.CONST_LIMIT_RECORDS > 0: df_1 = df_0.limit(self.CONST_LIMIT_RECORDS)
        else: df_1 = df_0
        df_2 = flatten_spark_dataframe(df_1)
        df = df_2.toPandas()
        """
        try: print(df['type'])
        except: pass

        #for i, col in enumerate( list(df.columns) ):
        #  print(i+1, col)
        
        """
        for col in df.columns:
          print("Column " + col + " has type of: " + str(df[col].dtype) )
          if df[col].dtype == 'object':
            try: df[col] = pd.to_datetime(df[col])
            except:
              try: df[col] = df[col].astype(int)
              except:
                try: df[col] = df[col].astype(float)
                except:
                  pass
          print("Column " + col + " now has type of: " + str(df[col].dtype) + " (" + str(df[col][0]) + ')\n' )
          
        #endfor
        """
         
        for col in df.columns:
          if df[col].dtype == 'object':
              print()
              print("Trying to convert to strings: " + col)
              df[col] = df[col].astype(str)
              try:
                df[col] = df[col].apply( lambda z: z.decode('ascii') )
                print("Converted to strings YAY: " + col)
                
              except:
                print()
                print("ERROR")
                print()
                print(list(df[col]))
                print("Converted to strings NO: " + col)
 
        for ns in CONFIG_inferNullStrings:
            df = df.replace({ns:CONFIG_inferNullReplace})
                 
         
        try:
            df['hour'] = df['created_at'].dt.hour
        except:
            pass
         
        # Save the full dataset to a CSV file???
        # df.to_csv("Profiling/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+num_records_string+"_theData.txt", sep="|" )
 
        if self.file.get().rfind('/') != -1: ftitle = self.file.get()[self.file.get().rfind('/')+1:]
        else: ftitle = self.file.get()
 
        # Get all the Data-Types that are present in this day's data (sub)set
  
        if dataSourceName == "gtir":
            datatype_divider_field = "data_type"
        elif dataSourceName == "scom":
            datatype_divider_field = "EventId"
        elif dataSourceName == "veramine":
            datatype_divider_field = "type"
        else:
            datatype_divider_field = "NONDEFINED-DATATYPE-DIVIDER-FIELD"
            
        try:
            data_types = df[datatype_divider_field].unique()
        except:
            data_types = ['ALLDATATYPES']
        
        print("data_types: ", data_types)
        
        sub_num_records = 0
        sub_num_records_str = str(sub_num_records)
        
        # Run Data Identification:
        # Moved outside of FOR loop because you don't
        # need to reconstruct Data Identities
        # for each subset of data. Its inefficient.
        #idFile = "Production/TestingV1/fields.json"
        idFile = "fields.json"
        DI = buildDataIdentities(idFile)
         
        # Run MetaData and Data-Profiling for each Data-Type (with max. of 'sub_num_records' records):
        for data_type in data_types:
            print("Now Processing DataType: " + data_type)
            
            if data_type == 'ALLDATATYPES':
                df_type = df
            else:
                df_type = df.loc[df[datatype_divider_field] == data_type]
            
            # Subset the DF to the correct subset length:
            if sub_num_records > 0:
                df_type = df_type.head(sub_num_records)
            
            # Run METADATA Here:
            self.generate_full_metadata(df_type, dataSourceName, dayDesired, numRecsDesired, sub_num_records_str, data_type)
            
            # Run Data Identifier against df_type
            DDI = dataIdentifier(df_type,DI)
             
            # Run Data-Profiler Here with Identifier results:
            DP = dataProfiler(df_type, DDI[0], DI) # printout=True
            
            for profile_field_name in DP.columns:
                DP[profile_field_name] = DP[profile_field_name].astype(str)
            
            # Write the PROFILING Output:
            dp_results = DP.reset_index(drop=False)
            # Use Reset Index to make the Index column part of the output.
            
            #profiling_SparkDF = spark.createDataFrame(dp_results)
            #print("Created profiling_SparkDF successfully.")
     
            ###profiling_SparkDF.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep="|").save(path="hdfs:///product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/output_"+dataSourceName+"_"+dayDesired+"_"+data_type+"_"+sub_num_records_str+"_profiling.txt")
            
            dp_results.to_csv("../outdata/profiling/profiling_" + self.inputname + "_" + str(self.CONST_LIMIT_RECORDS) + ".csv", sep="," )
            
            ##print("Wrote profiling_SparkDF to HDFS successfully.")

            """            
            # Write the IDENTITY Output:
            id_results = DDI[0].reset_index(drop=False)
            identify_SparkDF = spark.createDataFrame(id_results)
            print("Created identify_SparkDF successfully.")
                        
            identify_SparkDF.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep="|").save(path="hdfs:///product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/output_"+dataSourceName+"_"+dayDesired+"_"+data_type+"_"+sub_num_records_str+"_identifier.txt")
            print("Wrote identify_SparkDF to HDFS successfully.")
            """
            
            # Endpath specification and HTML Writing.
            #endpath = "Profiling/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+numRecsDesired+"_"+data_type+"_"+sub_num_records_str+"_profiling"
            
            endpath = "../outdata/profiling/profiling_" + self.inputname + "_" + str(self.CONST_LIMIT_RECORDS)
            createHTML(df_type,ftitle,DDI[0],DP,filepath=endpath)
        #endfor
        
        # Write NEW IDENTITY if not known data by re-running part of the identifier here.
        
        # Run identifier on WHOLE data set last to figure if ID needed.
        co = None
        DDI = dataIdentifier(df,DI)
        for d in DI:
            ind_columns = ["Column Count","Strict Comparison","Lazy Comparison","Intersection","Key Fields"]
            row = DDI[0].loc[d.getName()]
            com = pd.Series(data=[True,True,True,True,True],name=d.getName(),index=ind_columns,dtype=object)
            lcom = pd.Series(data=[True,False,True,True,True],name=d.getName(),index=ind_columns,dtype=object)

            if(row.equals(com) or row.equals(lcom)):
                co = d
                break
            else:
                #print("Identification continues...")
                continue
        if(co != None):
            # Known data, so don't do anything.
            print("Data was confirmed as: ",co)
        else:
            # Unknown data. use identityCreator() to create new data ID to write to file.
            print("Data was not confirmed, writing new Identity...")
            # TODO: Write JSON file
            # newDI = identityCreator(df_type.columns,tofile=False,tkinter=False)
            # 

 
        """
        # Analyse the data by Data Type & by Hour as well
        for data_type in ["relation"]:# ['ttp','report','course-of-action','exploit-target','threat-actor']: # ["indicator"]: ## ["relation"]:###### data_types:
            df_type = df.loc[df['data_type'] == data_type]
             
            for hourChosen in [12,13,14,15,16,17,18,19,20,21,22,23]:
                hourChosenStr = str(hourChosen)
                if len(hourChosenStr) == 1: hourChosenStr = "0" + hourChosenStr
             
                df_type_hour = df_type.loc[df_type['hour'] == hourChosen]
                df_type_hour = df_type_hour.head(250)
                 
                if len(df_type_hour.index) < 100:
                    continue
             
                data_type_hour = data_type + "_" + hourChosenStr
                 
                self.generate_full_metadata(df_type_hour, dataSourceName, dayDesired, num_records_string, data_type_hour)
                 
                DI = buildDataIdentities()
                DDI = dataIdentifier(df_type_hour,DI)
                DP = dataProfiler(df_type_hour,DDI[0],DI)
             
                for profile_field_name in DP.columns:
                    DP[profile_field_name] = DP[profile_field_name].astype(str)
             
                profiling_SparkDF = spark.createDataFrame(DP)
                print("Created profiling_SparkDF successfully.")
             
                # Write the PROFILING Output:
                profiling_SparkDF.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep="|").save(path="hdfs:///product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/output_"+dataSourceName+"_"+dayDesired+"_"+data_type_hour+"_"+num_records_string+"_profiling.txt")
                print("Wrote profiling_SparkDF to HDFS successfully.")
             
                endpath = "Profiling/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+data_type_hour+"_"+num_records_string+"_profiling"
                createHTML(df_type,ftitle,DDI[0],DP,filepath=endpath)
         
        #sys.exit()
        """
         
        """
        # Do the analysis by Hour
        for hourChosen in [12,13,14,15,16,17,18,19,20,21,22,23]:
            hourChosenStr = str(hourChosen)
             
            df_hour = df.loc[df['hour'] == hourChosen]
            df_hour = df_hour.head(100)
         
            self.generate_full_metadata(df_hour, dataSourceName, dayDesired, num_records_string, hourChosenStr)
 
            DI = buildDataIdentities()
            DDI = dataIdentifier(df_hour,DI)
            DP = dataProfiler(df_hour,DDI[0],DI)
 
            for profile_field_name in DP.columns:
                DP[profile_field_name] = DP[profile_field_name].astype(str)
 
            profiling_SparkDF = spark.createDataFrame(DP)       
            print("Created profiling_SparkDF successfully.")
     
            # Write the PROFILING Output:
            profiling_SparkDF.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep="|").save(path="hdfs:///product/cso/data_science/Profiling/DataSources/"+dataSourceName+"/output_"+dataSourceName+"_"+dayDesired+"_"+hourChosenStr+"_"+num_records_string+"_profiling.txt")
            print("Wrote profiling_SparkDF to HDFS successfully.")
 
            endpath = "Profiling/"+dataSourceName+"/"+dataSourceName+"_"+dayDesired+"_"+hourChosenStr+"_"+num_records_string+"_profiling"
            createHTML(df_hour,ftitle,DDI[0],DP,filepath=endpath)
        #endfor hourChosen
        """
         
    #enddef gendata
#endclass MetaFrame
 
 
def SQLLogToDataframe(filename):
    # Converts a textual printout of an SQL log into a dataframe.
     
    with open(filename,encoding='utf-8') as tf:
        file_data = tf.readlines()
        tf.close()
         
    raw_cols  = (file_data[1][1:-2])    # Raw file columns, including whitespace and pipes.
    raw_rows  = (file_data[3:-1])       # Raw file rows, including whitespace and pipes.
     
    file_cols = []                      # Where the cleaned columns go.
    file_rows = []                      # Where the cleaned rows go.
    err_rows  = []                      # Where rows which don't line up with the columns get sent.
 
    # Split and Strip for Columns since its only one "row"
    cols_split = raw_cols.split("|")
    for c in cols_split:
        c = c.strip()
        file_cols.append(c)
     
    # Two step process needed for rows, as its the data WITHIN the lists which needs processing.
    strip_rows = []
    infer_rows = []
     
    # Splitting
    for rr in raw_rows:
        d = rr[1:-2]
        d_split = d.split("|")
        strip_rows.append(d_split)
     
    # Stripping
    for sr in strip_rows:
        new_row = []
        for d in sr:
            nr = d.strip()
            new_row.append(nr)
        infer_rows.append(new_row)
     
    # Inferring Numbers
    for ir in infer_rows:
        new_row = []
        for i in ir:
            if(i.isnumeric()):
                ii_int = int(i)
                ii_flo = float(i)
                 
                if(ii_int == ii_flo):
                    i = ii_int
                else:
                    i = ii_flo
            else:
                pass
            new_row.append(i)
         
        if(len(new_row) == len(file_cols)):
            file_rows.append(new_row)
        else:
            err_rows.append(new_row)
         
        '''
        try:
            file_rows.append(new_row)
        except AssertionError:
            # If the number of columns in the record and the
            # number of columns in the header don't line up, this happens. Skip the line.
            pass
        '''
     
    print("\nError rows found:")
    for r in err_rows:
        print(r)
        print("Columns: "+str(len(r))+" (Not "+str(len(file_cols))+")\n")
    return pd.DataFrame(file_rows,columns=file_cols)
 
def CSVFileToDataframe(filename):
    # Converts a CSV document into a dataframe.
     
    return pd.read_csv(filename,index_col=False)
 
 
##########################
## DIP Code (by Andrew) ##[DIP]
##########################
def diptst(dat, is_hist=False, numt=1000):
    """ diptest with pval """
    # sample dip
    d, (_, idxs, left, _, right, _) = dip_fn(dat, is_hist)
    # simulate from null uniform
    unifs = np.random.uniform(size=numt * idxs.shape[0])\
                     .reshape([numt, idxs.shape[0]])
    unif_dips = np.apply_along_axis(dip_fn, 1, unifs, is_hist, True)
    # count dips greater or equal to d, add 1/1 to prevent a pvalue of 0
    pval = None \
      if unif_dips.sum() == 0 else \
        (np.less(d, unif_dips).sum() + 1) / (np.float(numt) + 1)
    return (d, pval, # dip, pvalue
            (len(left)-1, len(idxs)-len(right)) # indices
            )
  
def _gcm_(cdf, idxs):
    work_cdf = cdf
    work_idxs = idxs
    gcm = [work_cdf[0]]
    touchpoints = [0]
    while len(work_cdf) > 1:
        distances = work_idxs[1:] - work_idxs[0]
        slopes = (work_cdf[1:] - work_cdf[0]) / distances
        minslope = slopes.min()
        minslope_idx = np.where(slopes == minslope)[0][0] + 1
        gcm.extend(work_cdf[0] + distances[:minslope_idx] * minslope)
        touchpoints.append(touchpoints[-1] + minslope_idx)
        work_cdf = work_cdf[minslope_idx:]
        work_idxs = work_idxs[minslope_idx:]
    return np.array(np.array(gcm)), np.array(touchpoints)
  
def _lcm_(cdf, idxs):
    g, t = _gcm_(1-cdf[::-1], idxs.max() - idxs[::-1])
    return 1-g[::-1], len(cdf) - 1 - t[::-1]
  
def _touch_diffs_(part1, part2, touchpoints):
    diff = np.abs((part2[touchpoints] - part1[touchpoints]))
    return diff.max(), diff
  
def dip_fn(dat, is_hist=False, just_dip=False):
    """
        Compute the Hartigans' dip statistic either for a histogram of
        samples (with equidistant bins) or for a set of samples.
    """
    if is_hist:
        histogram = dat
        idxs = np.arange(len(histogram))
    else:
        counts = collections.Counter(dat)
        idxs = np.msort(list(counts.keys()))
        histogram = np.array([counts[i] for i in idxs])
    # check for case 1<N<4 or all identical values
    if len(idxs) <= 4 or idxs[0] == idxs[-1]:
        left = []
        right = [1]
        d = 0.0
        return d if just_dip else (d, (None, idxs, left, None, right, None))
    cdf = np.cumsum(histogram, dtype=float)
    cdf /= cdf[-1]
    work_idxs = idxs
    work_histogram = np.asarray(histogram, dtype=float) / np.sum(histogram)
    work_cdf = cdf
    D = 0
    left = [0]
    right = [1]
    while True:
        left_part, left_touchpoints = _gcm_(work_cdf-work_histogram, work_idxs)
        right_part, right_touchpoints = _lcm_(work_cdf, work_idxs)
        d_left, left_diffs = _touch_diffs_(left_part,
                                           right_part, left_touchpoints)
        d_right, right_diffs = _touch_diffs_(left_part,
                                             right_part, right_touchpoints)
        if d_right > d_left:
            xr = right_touchpoints[d_right == right_diffs][-1]
            xl = left_touchpoints[left_touchpoints <= xr][-1]
            d = d_right
        else:
            xl = left_touchpoints[d_left == left_diffs][0]
            xr = right_touchpoints[right_touchpoints >= xl][0]
            d = d_left
        left_diff = np.abs(left_part[:xl+1] - work_cdf[:xl+1]).max()
        right_diff = np.abs(right_part[xr:]
                            - work_cdf[xr:]
                            + work_histogram[xr:]).max()
        if d <= D or xr == 0 or xl == len(work_cdf):
            the_dip = max(np.abs(cdf[:len(left)] - left).max(),
                          np.abs(cdf[-len(right)-1:-1] - right).max())
            if just_dip:
                return the_dip/2.0
            else:
                return the_dip/2.0, (cdf, idxs, left, left_part, right, right_part)
        else:
            D = max(D, left_diff, right_diff)
        work_cdf = work_cdf[xl:xr+1]
        work_idxs = work_idxs[xl:xr+1]
        work_histogram = work_histogram[xl:xr+1]
        left[len(left):] = left_part[1:xl+1]
        right[:0] = right_part[xr:-1]
 
#############################
### Data Identity Classes ###[DID]
#############################
#%% Data Column Class
# Blank Column Dictionary Constant
# DON'T DELETE
CONST_blankCol = {"name":"New Column", "type":"string", "nullable":False, "min_length":0,
                  "max_length":0, "functions":[], "children":[]}
class DataColumn():
    # Holds column data for data identities.
    def __init__(self,newCol=CONST_blankCol):
        self._name      = newCol.setdefault("name",CONST_blankCol["name"])
        try:
            self.__id       = newCol["col_id"]
        except KeyError: 
            self.__id       = self.generateID()
        self._type      = newCol.setdefault("type",CONST_blankCol["type"])
        self._null      = newCol.setdefault("nullable",CONST_blankCol["nullable"])
        self._minlen    = newCol.setdefault("min_length",CONST_blankCol["min_length"])
        self._maxlen    = newCol.setdefault("max_length",CONST_blankCol["max_length"])
        self._funcs     = newCol.setdefault("functions",CONST_blankCol["functions"])
        
        # Unpack Children into new Data Identities
        children        = newCol.setdefault("children",CONST_blankCol["children"])
        childs          = []
        for c in children:
            try:
                # Try to process dictionary into object.
                childs.append(DataColumn(c))
            except AttributeError:
                # Encountered something already processed, so pass it through.
                childs.append(c)
        
        self._children  = childs
        
    def __repr__(self):
        return self.getName()+" (#"+self.getID()+")"
    
    def __len__(self):
        # Length getter
        return len(self._children)
        
    def __getitem__(self,index):
        # Gets a child data identity, just like *snap*
        try:
            return self._children[index]
        except IndexError:
            print("__getitem__: Index out of bounds.")
            return False
        except ValueError:
            print("__getitem__: Index not an integer.")
            return False
        except TypeError: 
            print("__getitem__: Wrong type supplied for index.")
            return False

    def __setitem__(self,index,new):
        # Item Setter
        self._children[index] = new
    
    '''
    def __iter__(self,):
        for c in self._children:
            return c
    
    def __next__(self,):
        if:
            pass
        else:
            raise StopIteration
    '''
    
    def generateID(self):
        # Instantiates a non-user Friendly unique ID for the Column
        chars = "0123456789ABCDEF"
        idLen = 12
        newID = "".join(random.choice(chars) for i in range(idLen))
        
        print("New ID for column "+self.getName()+": "+newID)
        return newID
    
    ## Getters and Setters
    def getID(self):
        return self.__id
    # No setter for IDs. They're special.
    
    def getName(self):
        return self._name
    def setName(self,newName=str):
        self._name = newName
    
    def getType(self):
        return self._type
    def setType(self,newType=str):
        self._type = newType
        
    def getNullable(self):
        return self._null
    def setNullable(self,newNull=bool):
        self._null = newNull
    
    def getMinLength(self):
        return self._minlen
    def setMinLength(self,newLen=int):
        self._minlen = newLen
    
    def getMaxLength(self):
        return self._maxlen
    def setMaxLength(self,newLen=int):
        self._maxlen = newLen
    
    def getFunctions(self):
        return self._funcs
    def setFunctions(self,newFuncs=[]):
        self._funcs = newFuncs
    
    def getChildren(self):
        return self._children
    
    def getAllChildren(self):
        # 2 Dimensional array.
        # Index 0 is root.
        
        '''
        childs = [self.getChildren()]
        for c in childs:
            childs.append(c.getAllChildren())
            
        return childs
        '''
    
    def addChild(self):
        print()
        return self._children.append(DataColumn())
    
    def countChildren(self):
        return len(self._children)
    
    def serialiseToJSON(self):
        # Construct Dictionary to Encode column data within data identity
        # I have no idea if this'll work recursively with how I've set it up...
        
        ccs = self.getChildren()
        childs = []
        
        for c in ccs:
            # Is this even _possible?_
            childs.append(c.serialiseToJSON())
        #print("Children Found under "+self.getName()+": "+str(childs))
        
        jsonObj = dict(col_id=self.getID(),
                       name=self.getName(),
                       type=self.getType(),
                       nullable=self.getNullable(),
                       min_length=self.getMaxLength(),
                       max_length=self.getMinLength(),
                       functions=self.getFunctions(),
                       children=childs
                      )
        # Funnily enough, something made from JSON translates
        # straight back into JSON very easily.
        
        # Run encoder on Dictionary.
        return jsonObj

    @classmethod
    def serialiseListToJSON(self,coList=[]):
        # This is needed to turn a list of Column Datas into records for
        # being serialised into JSON.
        # Works in conjunction with the Data-Identity level serialise.
        
        ret = []
        for c in coList:
            ret.append(self.serialiseToJSON(c))
        
        return ret
    
    @classmethod
    def unpackDictsIntoObjects(self,dicts=[]):
        # Turns a list of dictionaries which contain
        ret = []
        
        for di in dicts:
            ret.append(DataColumn(di))
        
        return ret

#%% Data Identity Class
class DataIdentity():
    #########################
    # Holds data type records for ease of analysis.
    
    def __init__(self,newFields):
        # Insulate data identity from misloading non-existent data with try/except :V
        
        self.name        = newFields.setdefault("name","New identity")
        self.desc        = newFields.setdefault("desc","Blank Description")
        self.field_count = newFields.setdefault("field_count",1)
        self.key_fields  = newFields.setdefault("key_fields",[])
        self.null_string = newFields.setdefault("null_string",["","null"])
        
        # Column Data/Column Unpacking
        cols             = newFields.setdefault("columns",CONST_blankCol)
        self.columns     = DataColumn.unpackDictsIntoObjects(cols)
        
        # Timekeeping Functions
        try:    self.created     = newFields["created"]
        except: self.created     = getTimeStr()
        try:    self.updated     = newFields["updated"]
        except: self.updated     = getTimeStr()
        
        # Timestamp failure prevention.
        try:    self.timestamp   = newFields["timestamp"]
        except: self.timestamp   = ""
    
    def __repr__(self):
        # What to use when representing oneself.
        return self.name
    
    def __getitem__(self,index):
        # Gets a data column, just like *snap*
        try:
            return self.columns[index]
        except IndexError:
            print("__getitem__: Index out of bounds.")
            return False
        except ValueError:
            print("__getitem__: Index not an integer.")
            return False
        except TypeError: 
            print("__getitem__: Wrong type supplied for index.")
            return False
    
    def __setitem__(self,index,new):
        # Item Setter
        self.columns[index] = new
    
    def __len__(self):
        # Length getter
        return len(self.columns)
    
    #%% Getter/Setter Functions
    #############################
    # DONE: Transform Getters into Getter/Setters where possible/logical

    def getName(self,newName=None):
        # Write new and/or return name.
        if(newName != None and isinstance(newName,str)):
            self.name = newName
        return self.name

    def getDesc(self,newDesc=None):
        # Write new and/or return description.
        if(newDesc != None and isinstance(newDesc,str)):
            self.desc = newDesc
        return self.desc
    
    def getFieldCount(self):
        # Count and Return number of Columns.
        # Can't be overwritten - must be derived from number of Columns.
        self.field_count = len(self.columns)
        return self.field_count
    
    def getKeyFields(self,newKeys=None):
        # https://www.geeksforgeeks.org/args-kwargs-python/
        # Return key fields list.
        if(newKeys != None and isinstance(newKeys,list)):
            print("New Key Fields: "+str(newKeys))
            self.key_fields = newKeys
        return self.key_fields
    
    def getNullString(self,newNulls=[]):
        # Return null string
        if(newNulls != []):
            print("New Null Strings: "+str(newNulls))
            self.null_string = newNulls
        return self.null_string
    
    def getNullStrings(self,newNulls=[]):
        # Alias for getNullString()
        # Because its easy to get the two mixed up.
        return self.getNullString(newNulls)
    
    def getTimestamp(self,newStamp=""):
        # Gets the timestamp.
        if(newStamp != ""):
            print("New Timestamp: "+str(newStamp))
            self.timestamp = newStamp
        return self.timestamp
    
    #%% Time Functions
    ####################
    def updateTime(self):
        # Update when the Data Identity was last modified.
        self.updated = getTimeStr()
    
    def getUpdateTime(self):
        return self.updated
    
    def creationTime(self):
        self.created = getTimeStr()
    
    def getCreationTime(self):
        return self.created
    
    #%% Data Analysis Getters
    ###########################
    def getColumnData(self):
        # Return list of column dictionaries.
        return self.columns
    
    def getColumnDataAll(self):
        # Get all column data recursively.
        array = []
        for c in self.columns:
            array.append(c.getChildren())
    
    def getFieldNames(self):
        # Return a list of column names from field dictionaries at base level.
        result = []
        for f in self.columns:
            result.append(f.getName())
        return result
    
    def getFieldFuncs(self):
        # Return a list of lists of functions to run at base level.
        result = []
        for f in self.columns:
            result.append(f.getFunctions())
                    
        return result

    #%% Column Data Functions
    ###########################
    # TODO: Fix these up to make them work with new indexing system.
    
    def getColumnByIndex(self,index=int(0)):
        # Get a dict of column data by specifying an index integer.
        # Returns a Column Dict, has error handling.
        return self[index]
    
    def addColumn(self):
        # Add a new, blank column to the data identity.
        self.columns.append(DataColumn(CONST_blankCol.copy()))
        self.getFieldCount()
        return True
    
    def removeColumn(self,index=int(0)):
        # Remove a column from the fields list.
        del self.columns[index]
        self.getFieldCount()
    
    def removeColumnByID():
        pass
    
    def updateColumn(self,index=int(0),
                     name="",dtype="",nullable=False,
                     minLen=0,maxLen=0,funcs=[],children=[]):
        # Specify a column by index, then use keyword arguments to update item.
        # Does _everything_. Will split this into cleaner chunks.
        rec = self.columns[index]
        
        rec["name"]       = name
        rec["type"]       = dtype
        rec["nullable"]   = nullable
        rec["min_length"] = minLen
        rec["max_length"] = maxLen
        rec["functions"]  = funcs
        rec["children"]   = children
    
    def updateColumnName(self,index=int(0),newName=""):
        if(newName is not ""):
            rec = self[index]
            rec.setName(newName)
            print("updateColumnName: Name changed.")
        else:
            print("updateColumnName: No name specified.")
    
    def updateColumnType(self,index=int(0),newType=""):
        if(newType is not ""):
            rec = self[index]
            rec.setType(newType)
            print("updateColumnType: Type changed.")
        else:
            print("updateColumnType: No type specified.")
    
    def updateColumnNullable(self,index=int(0),newStatus=None):
        if(newStatus is not None):
            rec = self[index]
            rec.setNullable(newStatus)
            print("updateColumnNullable: Status updated.")
        else:
            print("updateColumnNullable: New nullable status isn't a bool.")
       
    def updateFuncs(self,index=int(0),newFuncs=[]):
        # Updates the current list of functions.
        print("NEW FUNCTIONS: "+str(newFuncs))
        self.columns[index].setFunctions(newFuncs)
    
    def moveColumn(self,oldIndex=int(0),newIndex=int(0)):
        # Change a column's position by specifying which record to move
        # by index (oldIndex) and where you want it to go (newIndex)
        failed = False
        
        colLen = len(self.columns)
        
        try:
            col = self[oldIndex]
        except IndexError:
            print("ShiftColumn: Old Index out of bounds.")
            failed = True
        except ValueError:
            print("ShiftColumn: Old Index not an integer.")
            failed = True
        
        # If previous step failed, don't proceed
        # with deletion and reinsertion.
        if(failed == False):
            try:
                wrap = False # Will be set to "up" or "down", depending.
                
                if(newIndex == colLen):
                    # Make sure it goes from bottom to 0.
                    wrap = "down"
                elif(newIndex == -1):
                    # Make sure it loops back around
                    # in case the new index is negative.
                    wrap = "up"
                    
                if(wrap == False):
                    # Otherwise, proceed as normal with remove/insert.
                    print("ShiftColumn: Shift successful.")
                    self.columns.remove(col)
                    self.columns.insert(newIndex,col)
                elif(wrap == "down"):
                    print("ShiftColumn: Wrapping from bottom to top.")
                    self.columns.remove(col)
                    self.columns.insert(0,col)
                elif(wrap == "up"):
                    print("ShiftColumn: Wrapping from top to bottom.")
                    self.columns.remove(col)
                    self.columns.insert(colLen,col)
                else:
                    print("ShiftColumn: Wrapping failed.\nThat wasn't meant to happen.")
                    
            except IndexError:
                print("ShiftColumn: New Index out of bounds.")
            except ValueError:
                print("ShiftColumn: New Index not an integer.")
        else:
            print("ShiftColumn: Aborting. No changes made.")
    
    #%% Other Functions
    #####################

    def describe(self):
        # Describe self and traits in light detail.
        print("I am "+self.name+".")
        print(self.desc)
        print("I contain "+str(self.field_count)+" fields:")
        for f in self.columns:
            print(f.getName())
            
    #%% Serialise/Save Functions
    ##############################
    def serialiseToJSON(self):
        # https://pythonspot.com/json-encoding-and-decoding-with-python/
        # Serialises self, returns JSON string representation.
        # Prepare JSON Encoder
        
        colJ = DataColumn.serialiseListToJSON(self.getColumnData())
        
        # Construct Dictionary to Encode
        jsonObj = dict(name=self.getName(),
                       desc=self.getDesc(),
                       created=self.created,
                       updated=self.updated,
                       field_count=self.getFieldCount(),
                       key_fields=self.getKeyFields(),
                       null_string=self.getNullString(),
                       timestamp=self.getTimestamp(),
                       columns=colJ
                      )
        # Funnily enough, something made from JSON translates
        # straight back into JSON very easily.
        
        # Run encoder on Dictionary.
        return jsonObj
    
    #%% Class Methods
    ###################
    @classmethod
    def generateIdentities(self,filename="fields.json"):
        # https://pythonspot.com/json-encoding-and-decoding-with-python/
        # Class method to get all people inside filename
        # and return them as a list of data identities.
        result = []
        try:
            with open(filename) as f:
                json_list = json.load(f)
                f.close()
                
            for i in json_list:
                di = DataIdentity(i)
                result.append(di)
        except FileNotFoundError:
            # File not found.
            print("generateIdentities: Field File not found.")
        except PermissionError:
            # File has permission issues.
            print("generateIdentities: Permission error with Field File.")
        except OSError:
            # File has other problems...
            print("generateIdentities: Encountered OSError. Cannot read Field File.")
            
        print("Identities Generated: ",result)
        return result
    
    @classmethod
    def serialiseListToJSON(self,didList=[]):
        # Turn all data identities in a list into Serialised JSON strings.
        # Ideal for putting all the ducks in a row before saving/writing.
        
        ret = []
        for di in didList:
            ret.append(di.serialiseToJSON())
        
        enc = JSONEncoder(skipkeys=True,indent="")
        return enc.encode(ret)

#%% External Functions
# Example Blank Data Identity for creating new Data IDs
def getBlankDI(blankCol=True):
    # Create a blank data identity with current date and time, and a blank column.
    if blankCol == True:
        return DataIdentity({"name": "New identity", "desc": "Blank Description",
                             "created": getDITimeStr(), "updated": getDITimeStr(),
                             "field_count": 0, "key_fields": [],
                             "time_stamp": "", "null_string": ["null",""],
                             "columns": [CONST_blankCol]}
                           )
    else:
        return DataIdentity({"name": "New identity", "desc": "Blank Description",
                             "created": getDITimeStr(), "updated": getDITimeStr(),
                             "field_count": 0, "key_fields": [],
                             "time_stamp": "", "null_string": ["null",""],
                             "columns": []}
                           )

def getDITimeStr(timeForm='%Y-%m-%d %H:%M:%S'):
    # Gets the current time, returns as a string.
    time = dt.now().strftime(timeForm)
    return str(time)


#%% Data ID Creator
def identityCreator(columns=[],tofile=False,tkinter=False):
    # Quick and dirty function!
    # Creates a flat Data Identity, which can be filled
    #  in later via the Data Identiy Build Tool.
    did   = getBlankDI(False)
    idlen = range(len(columns))
    clist = []
    
    # Create list of numbers to be populated
    for n in idlen:
        clist.append(n)
    # Set that list to Column Dicts
    did.columns = clist
    # Populate that list with Data Column Objects
    for n in idlen:
        did[n] = DataColumn({"name":columns[n]})
    
    # Now we save the file depending on args passed.
    if(tkinter == True):
        # To be used if this function's called in a Tkinter context.
        # Otherwise don't do this, it'll lead to 'funny' things.
        from tkinter import messagebox
        from tkinter import filedialog
        
        yousure = messagebox.askyesno(title="Create Data Identity",
                              message="Save Data Identity to file?")
        
        if(yousure):
            filepath = filedialog.asksaveasfile(initialdir="./",
                                                initialfile="New Data Identity",
                                                filetypes=[("JSON","*.json")],
                                                defaultextension=".json",
                                                title="Save Data Identity Library")
            try:
                if(filepath.name != "" or filepath == None or filepath != ""):
                    with open(filepath.name,"w") as f:
                        f.write(DataIdentity.serialiseListToJSON([did]))
                    print("saveAsLibrary: File saved.")
                else:
                    print("saveAsLibrary: Blank filepath.")
            except AttributeError:
                print("saveAsLibrary: Attribute Error.")
        else:
            print("Data ID not saved.")
    
    if(tofile == True):
        # Use this if not within a Tkinter context.
        # Will write directly to file in the same folder as the program.
        import os
        
        # Get working directory
        wd = os.getcwd()
        fn = "output_"+getTimeStr('%Y-%m-%d %H%M%S')+".json"
        with open(wd+"\\"+fn,"w") as f:
            f.write(DataIdentity.serialiseListToJSON([did]))
    
    # In either case, return the Data Identity.
    return did
 
########################################
### General Data Profiling Functions ###[DPF]
########################################
# Timeform Data For formatting common datetime strings, with an example below
timeForm_ymdhmsf = "%Y-%m-%d %H:%M:%S.%f"   # Full date and time plus milliseconds.
timeForm_ymd     = "%Y-%m-%d"               # Just the date, with dashes.
timeForm_hmsf    = "%H:%M:%S.%f"            # Just the time plus milliseconds.
timeForm_ymdhms  = "%Y-%m-%d %H:%M:%S"      # Full date and time minus miliseconds.
 
timeForms_long   = [timeForm_ymdhmsf,timeForm_ymd,timeForm_hmsf,timeForm_ymdhms]
''' 2019-11-30 01:45:29.000 '''
 
# Redundant/compressed date (rd*) timeforms.
timeForm_rdy     = "%Y"                     # Redundant Date: Year Only.
timeForm_rdm     = "%Y%m"                   # Redundant Date: Year + Month.
timeForm_rdd     = "%Y%m%d"                 # Redundant Date: Year + Month + Day.
timeForm_rdh     = "%Y%m%d%H"               # Redundant Date: Year + Month + Day + Hour.
 
timeForms_rd    = [timeForm_rdy,timeForm_rdm,timeForm_rdd,timeForm_rdh]
''' 2019-11-30 01:35:53, 2019113001 '''
 
timeforms_all = [timeForm_ymdhmsf,timeForm_ymd,timeForm_hmsf,timeForm_ymdhms,timeForm_rdy,timeForm_rdm,timeForm_rdd,timeForm_rdh]
 
def isValueNull(value):
    # Checks if a value being passed is a "string null" or blank value.
    # Will turn it into a Python None (which is their version of Null)
 
    # Is it a string?
    if(isinstance(value,str)):
        # It is a string. Is it equal to a null?
        if(value == "" or value == "null"):
            return None
        else:
            return value
    else:
        # Not a string.
        return value
 
def isValueArray(value):
    # Checks if a value being passed is an array by the existence of the following symbols:
    # Commas (for file separation), and Square Brackets at either end for defining a list/array.
    # Returns a list if it is an array, or the original input.
    if(isinstance(value,str)):
        # Check if it is an array.
        check = (value[0] == "[" and value[-1] == "]" and "," in value)
        if(check):
            strip_val = value[1:-2]
            value = strip_val.split(",")
            return value
        else:
            return value
    else:
        return value
 
def countingDict(d,v,c=1):
    # Creates or updates a counting dictionary, using the key (v) as a name and its value for count (c).
    # Requires a dictionary to be specified, and key name to use.
     
    try:
      v_enc = v.encode('utf-8')
    except:
      v_enc = v
    if( v_enc in d):
        # Dictionary entry exists. Add to its existing numeric value.
        d[v_enc] += c
    else:
        # Dictionary entry doesn't exist. Initialise it and start at given value.
        d[v_enc] = c
 
def stringToTime(timeString,timeForms=[]):
    # Attempt to process a string which represents a date/time,
    # and return a datetime object, otherwise return string only.
 
    for f in timeForms:
        # Give each given timeform within iterable a whirl, and return just the string if it doesn't succeed at all.
        try:
            return dt.strptime(timeString,timeForm)
        except ValueError:
            # Time string or Time format malformed. Return original string string only.
            #print("StringToTime: Value Error encountered. Time format string \""+timeForm+"\" unsuitable for \""+timeString+"\"")
            continue
 
    # If all else fails, return original string...
    return timeString
 
###############################################
## Dataframe/Column Data Profiling Functions ##[DFC]
###############################################
def countUniqueValues(colList):
    # Iterate through a column of records and counts the unique records from that column.
    # Returns a tuple with two counts, the non-unique values list, and a percentage of unique values.
    uniques = []
    non_uniques = {}
    for f in colList:
        if(f not in uniques and f is not None and f != None):
            # Add unique value to unique list.
            uniques.append(f)
        else:
            # Add non-unique value to non-unique list.
            countingDict(non_uniques,f)
    ##print("Uniques: ", uniques[:8])
    return (len(uniques),len(non_uniques),non_uniques,round((len(uniques)/float(len(colList)))*100,2), uniques, sum(non_uniques.values()))
     
def countNullValues(colList):
    # Iterate through a column of data believed to contain nulls.
    # And count all instances of null/None in each value row.
    # Returns an integer with the number of nulls present.
    # And a percentage of null values.
    num_null = 0
    for f in colList:
        if(f is None or f==None):  # Null hs been found:
            num_null += 1
        else:
            continue
    try:
        percent_null = round((float(num_null)/len(colList))*100,4)
    except:
        print("In count null vals, colList is: " + str(colList))
        percent_null = 0
    return num_null, percent_null
 
def countMaxMinLength(colList):
    # Iterate through a column of records, and find the longest and shortest values possible.
    # Returns a tuple with two integers for shortest and longest length found.
     
    resultMax = float("-inf")
    resultMin = float("inf")
    lengthList = []
     
    for f in colList:
        if(f == None):
            # A null counts as a big, fat 0.
            # Guaranteed not to overwrite an existing high value.
            length = 0
        else:
            # Not null, so count length of value.
            if(type(f) == float):
                try:    length = len(str(f))
                except: length = 0
            else:
                try:
                    length = len(str(f))
                except:
                    continue
             
            lengthList.append(length)
     
    for l in lengthList:
        if l > resultMax:
            resultMax = l
         
        if l < resultMin:
            resultMin = l
     
    return (resultMin,resultMax)
 
def countAverageLength(colList):
    # Iterate through a column of records to find the average string length
    # of the column's data.
    # Returns an int/float rounded to 5 places with record length average, and standard deviation.
     
    result = 0
    resultStack = []
    total = 0
     
    for f in colList:
        if(f == None):
            # Null values count as a 0.
            length = 0
        else:
            # Not null, so count length.
            if(type(f) == float):
                try:    length = len(str(f))
                except: continue
            else:
                try:    length = len(str(f))
                except: continue
            resultStack.append(length)
     
    for v in resultStack:
        total += v
     
    try:
        result = float(total)/len(resultStack)
    except ZeroDivisionError:
        result = 0
     
    try:
        stdev_length = np.std(resultStack) # ...=stats.stdev(resultStack) # no 'stats' in py3 cdsw
    except: # stats.StatisticsError:
        stdev_length = 0
     
    return (round(result,4), round(stdev_length,4) )
 
def countAverageNumber(colList):
    # Iterate through a column of records and figure out an average
    # numeric value. This won't work on a column/list of strings.
    # Returns an int/float with the result.
     
    result = 0
    resultStack = []
     
    for f in colList:
        try:
            if np.isnan(f):
                continue        # Exclude NaN values
            resultStack.append(float(f))
        except (TypeError, ValueError):
            # Not possible to cast as number. Reject.
            continue
     
    total = 0
    for v in resultStack:
        total += v
     
    try:
        result = float(total)/len(resultStack)
        result = round(result,4)
         
        if result == round(result): result = int(result)
         
    except ZeroDivisionError:
        return "N/A (No Numeric Data)"
     
    return result
 
def countMaxMinValues(colList, col):
    # Iterate through a list of data, and find the "highest" and "lowest" values numerically.
    # Returns a tuple with minimum, percentiles 25, 50 and 75, and a maximum.
    # Was originally alphanumeric, but got revised.
     
    vals = []
    minResult = None
    maxResult = None
    perc_25   = None
    perc_50   = None
    perc_75   = None
     
    for f in colList:
        try:
            x = np.isnan(f)
            y = pd.isnull(f)
        except:
            continue
        if(np.isnan(f) or pd.isnull(f) or f is None or f == None):
            # Effectively empty. Shouldn't be evaluated.
            pass
        elif(isinstance(f,list)):
            # Lists/arrays within data can't be handled.
            # Ignore for now.
            pass
        elif(isinstance(f,(int,float))):
            # Looks good to me! Add it to stack.
            vals.append( float(f) )
        else:
            try:
                f_as_float = float(f)
                vals.append(f_as_float)
            except:
                pass
     
    # Sort the values out.
    vals = sorted(vals)
     
    if(len(vals) > 0):
        # At least one result.
        minResult = vals[0]
        perc_25   = vals[int(len(vals) * 0.25)]
        perc_50   = vals[int(len(vals) * 0.50)]
        perc_75   = vals[int(len(vals) * 0.75)]
        maxResult = vals[-1]
         
        if minResult == round(minResult): minResult = int(minResult)
        if perc_25   == round(perc_25):   perc_25 =   int(perc_25)
        if perc_50   == round(perc_50):   perc_50 =   int(perc_50)
        if perc_75   == round(perc_75):   perc_75 =   int(perc_75)
        if maxResult == round(maxResult): maxResult = int(maxResult)
         
        return (minResult,maxResult,perc_25,perc_50,perc_75)
 
    else:
        fail = "N/A (No Numeric Data)"
        return (fail,fail,fail,fail,fail)
 
def countCommonValues(colList):
    # Iterate through a column of data, then count the number of non-null value occurrences.
    # Then pick the highest counting, lowest counting, and top 5/bottom 5 results.
    # Returns a tuple with least common, most common, bottom 5 and top 5.
     
    vals = {}
    for f in colList:
        # Count all not null values.
        if(f != None):
            try:
                countingDict(vals,f)
            except:
                continue
    sorted_vals = sorted(vals,key=vals.__getitem__,reverse=True)
     
    # Sort dictionary out using the dict counting values as the key.
    # Unfortunately it isn't perfect - it'll get _one_ of the least commonly occurring values.
    if(len(sorted_vals) > 0):
        leastCommon = sorted_vals[-1]
        mostCommon  = sorted_vals[0]
         
        bottom5     = sorted_vals[-5::]
        top5        = sorted_vals[0:5]
         
        bottom5.reverse()
        return (leastCommon,mostCommon,bottom5,top5)
    else:
        fail = "N/A (No Available Data)"
        return (fail,fail,fail,fail)
 
def countStandardDeviation(colList):
    # Iterate through a column of data, counting numerical values
    # to find a standard deviation.
    # Returns a float of standard deviation, or an error message if there's no numeric data.
     
    number_array = []
     
    for f in colList:
        # Is it a number?
        try:
            if np.isnan(f):
                continue      # Do not count NaN values, continue to next f value
            number_array.append(float(f))
        except:
            # f appears to be stringlike, but could still represent a numeric
            try:
                number_array.append(float(f))
            except:
                continue
     
    if(len(number_array) > 0):
        # There's numbers!
        try:
            return round(np.std(number_array),4)  ## stats.stdev(number_array)
        except: # stats.StatisticsError:
            return "N/A (St.Dev Error)"
    else:
        # There's not enough numbers!
        return "N/A (No Numeric Data)"
 
def findDataTypes(colList):
    # Iterate through a column of data, then count occurring data types.
    # Returns counting dictionary with number of encountered data types.
    # Bit hacky, but hey.
     
    dataTypes = {}
     
    # Begin the evaluatening of the data.
    for f in colList:
        # Is it a list?
        if(isinstance(f,list)):
            # It is. Mark it and continue.
            countingDict(dataTypes,"Array")
            continue
         
        # Is it a null?
        if(f == None):
            # It is. Mark it and continue.
            countingDict(dataTypes,"Null")
            continue
         
        if(f.isnumeric() == True):
            f_int = int(f)
            f_flo = float(f)
             
            # What sort of number is it?
            if(f_int != f_flo):
                # Yes. Its a float.
                countingDict(dataTypes,"Float")
                continue
            else:
                # Yes. Its an int.
                countingDict(dataTypes,"Integer")
                continue
     
        # Is it a date?
        isDate = False
         
        for tf in timeforms_all:
            # Try and process the data as a time string.
            try:
                dt.strptime(f,tf)
                isDate = True
                break
            except ValueError:
                # Unsuccessful time conversion
                # moves to next available timeform.
                continue
         
        # Otherwise, after all that, it must be a string.
        countingDict(dataTypes,"String")
     
    # Iteration finished. Return the dictionary.
    return dataTypes
 
def countCommonLength(colList):
    # Iterate through a list and get a list of data lengths.
    # Then find the most common and least common lengths.
     
    vals = {}
     
    for f in colList:
        if(f == None):
            # Null, counts as a 0.
            length = 0
        else:
            # Not null, count length of value.
            try:
                length = len(f)
            except:
                try:
                    length = len(str(f))
                except:
                    continue
            countingDict(vals,length)
     
    # Crunch numbers.
    sorted_vals = sorted(vals,key=vals.__getitem__,reverse=True)
     
    leastCommon = float(sorted_vals[-1])
    mostCommon  = float(sorted_vals[0])
     
    bottom5     = [ float(v) for v in sorted_vals[-5::] ]
    top5        = [ float(v) for v in sorted_vals[0:5] ]
     
    bottom5.reverse()
     
    return (leastCommon,mostCommon,bottom5,top5,vals)
 
###################################
## Functions Made By: Andrew Fox ##[AFOX]
###################################
# Integrated/adapted by yours truly. Thanks Andrew!
 
 
def getNearZeroVariance(colList):
    # Figures out if Near Zero Variance is true or false.
    # Returns a Boolean True or False.
     
    col = pd.Series(colList)
     
    try:
        most_common_count = col.value_counts().values[0]
    except IndexError:
        most_common_count = 0
 
    try:
        second_most_common_count = col.value_counts().values[1]
    except IndexError: # IE: Only one value present, so an index of 1 doesn't exist
        second_most_common_count = 0
     
    # Find the ratio of the most common to the second most common value.
    if(second_most_common_count == 0):
        ratio_most_common_to_second_most_common = sys.float_info.max
    else:
        ratio_most_common_to_second_most_common = (most_common_count/float(second_most_common_count))
 
    # Calculate Variance Percentage.
    try:
        percent_unique = ((col.nunique()/float(col.size))*100)
    except:
        percent_unique = 0
     
    # Now determine near zero variance.
    if(percent_unique < 10 and ratio_most_common_to_second_most_common > 20):
        near_zero_variance = True
    else:
        near_zero_variance = False
     
    return near_zero_variance
 
def getModality(colList):
    # Figures out modality, which is whether sequential numeric data has more than one "peak"
    # Returns a string with "Unimodal", "Multimodal" or "Non-numeric".
     
    # Check if the list's numeric by doing a list comprehension, returning a list of TRUE and FALSEs.
    data_check = [x for x in colList if isinstance(x,(int,float))]
     
    #print(colList[0:256])
    #print(data_check)
     
    if(len(data_check) > 0 and len(data_check) == len(colList)):
        # Cast our list as a Series instead, so we can do magic to it.
        dc_ser = pd.Series(data_check)
        dc_ser = dc_ser.dropna()
         
        # Not sure what sort of magic's afoot below, but...
        try:
            (d, pval, (len_left_minus_1, len_idxs_minus_len_right)) = diptst(dc_ser)
        except:  # diptst failed, so must be partially non-numeric data
            return "Non-numeric"
         
        # How come this keeps changing every time I run this? Round it down at least!
        if(pval != None):
            pval = round(pval,3)
        #print("PVAL: "+str(pval))
         
        # Once it figures out something, it'll return Unimodal OR Multimodal.
        if pval is None:   modality = "Unimodal"
        elif pval <= 0.1:  modality = "Multimodal"
        else:              modality = "Unimodal"
    else:
        # Its not a number, so modality can't be established.
        modality = "Non-numeric"
     
    return modality
 
def getKeyCandidate(colList):
    # This column is a Key Candidate if >95% values are Unique and < 0.5% values are Null
    if countNullValues(colList)[1] < 0.5 and countUniqueValues(colList)[3] > 95.0:
        is_key_candidate = True
    else:
        is_key_candidate = False
    return is_key_candidate
 
def getKeyStrengths(colList):
    def getCandidateKeyStrength(colList):
        '''
        The number of unique values divided by the total number of values in the column
        '''
        num_unique   = countUniqueValues(colList)[0]
        num_TOTAL    = len(colList)
        key_strength = ((num_unique)/float(num_TOTAL))*100.0
        key_strength = max([0.00, key_strength])
        return         key_strength
  
    def getCandidateKeySupport(colList):
        '''
        The number of unique values
        '''
        num_unique   = countUniqueValues(colList)[0]
        num_nulls    = countNullValues(colList)[0]
        num_TOTAL    = len(colList)
        key_support  = num_unique
        key_support  = max([0, key_support])
        return         key_support
      
    def getCandidateKeyViolations(colList):
        '''
        The number of duplicated values (counted once each) plus the number of Nulls
        '''
        num_TOTAL       = len(colList)
        num_unique      = countUniqueValues(colList)[0]
        dups_plus_nulls = (num_TOTAL - num_unique)
        num_violations  = min([dups_plus_nulls, num_TOTAL])
        return            num_violations
      
    def getCandidateKeyViolationsPercentage(colList):
        '''
        The percentage of duplicated values (counted once each) plus the number of Nulls
        '''
        num_TOTAL       = len(colList)
        num_violations  = getCandidateKeyViolations(colList)
        perc_violations = min([100.0*(num_violations/float(num_TOTAL)), 100.0])
        return            perc_violations
     
    '''
    "key_strength":                     results_keystr[0],
    "key_support_count":                results_keystr[1],
    "key_violation_count":              results_keystr[2],
    "key_violation_perc":               results_keystr[3],
    '''
     
    return (getCandidateKeyStrength(colList),getCandidateKeySupport(colList),getCandidateKeyViolations(colList),getCandidateKeyViolationsPercentage(colList))
 
def getDeterminedByColumns(dataFrame,colIndex):
    # Get Column Names and Indices.
    all_column_names = list(dataFrame.columns)
    column_name = all_column_names[colIndex]
    determined_by_columns = []
     
    # Enumerate the column indices and names to do comparison.
    for (other_column_index, other_column_name) in enumerate(all_column_names):
        #other_determines_column = doesAdetermineB( dataFrame[other_column_name] , dataFrame[column_name])
        other_determines_column = doesAdetermineB_byIndex(dataFrame, other_column_index, colIndex)
        if other_determines_column:
            # Append simply 'i' because we are going to use columns no's starting at '0'
            determined_by_columns.append(other_column_index)
    return( str(determined_by_columns) ) 
 
def getDeterminedColumns(dataFrame,colIndex):
    # Get Column Names and Indices.
    all_column_names = list(dataFrame.columns)
    column_name = all_column_names[colIndex]
    determined_columns = []
     
    # Enumerate the colum indices and names to do comparison.
    for (other_column_index, other_column_name) in enumerate(all_column_names):
        #column_determines_other = doesAdetermineB( dataFrame[column_name] , dataFrame[other_column_name])
        column_determines_other = doesAdetermineB_byIndex(dataFrame, colIndex, other_column_index)
        if column_determines_other:
            # Append simply 'i' because we are going to use columns no's starting at '0'
            determined_columns.append(other_column_index)
    return( str(determined_columns) )
 
def getDependencyStrengths(dataFrame,colIndex):
    # Get all column names and print them to a list.
    all_column_names = list(dataFrame.columns)
    column_name = all_column_names[colIndex]
     
    # Init lists for results
    strength_list = []
    support_list = []
    support_perc_list = []
    violations_list = []
    violations_perc_list = []
     
    # Begin enumerating through all of the columns in the dataframe against colIndex.
    for (other_column_index, other_column_name) in enumerate(all_column_names):
        # Use A_determines_B_strength_byIndex() to determine which columns
        # are supportive, and which ones violate dependency rules.
        (strength,support,support_perc,violations,
         violations_perc) = A_determines_B_strength_byIndex(dataFrame, colIndex, other_column_index)
         
        # Write results to the lists.
        strength_list.append(strength)
        support_list.append(support)
        support_perc_list.append(support_perc)
        violations_list.append(violations)
        violations_perc_list.append(violations_perc)
     
    # Return results.
    return((str(strength_list),
            str(support_list),
            str(support_perc_list),
            str(violations_list),
            str(violations_perc_list)
            ))
 
def doesAdetermineB_byIndex(dataFrame, column_index_A, column_index_B):
    # Get all Column names from the dataFrame.
    all_column_names = list(dataFrame.columns)
     
    # Get Columns to scan by Index.
    column_name_A = all_column_names[column_index_A]
    column_name_B = all_column_names[column_index_B]
     
    # Use those names to assign columns of data from the frame.
    colvals_A = dataFrame[column_name_A]
    colvals_B = dataFrame[column_name_B]
     
    # Run does A determine B to get a result.
    return doesAdetermineB(colvals_A, colvals_B)
 
def A_determines_B_strength(A,B):
    # Cast dataFrame columns to lists and init results dicts.
    A = A.tolist()
    B = B.tolist()
    results = {}
    primary_A_counts = {}
     
    # Begin the determining to populate results
    for (i,a) in enumerate(A):
        #print( i, a )
        try:
            # Replace NaN/Null value with Infinity to keep comparisons moving.
            if a is None or a == None or np.isnan(a):
                a = float("inf")
        except:
            pass
         
        # Get the corresponding value in B
        b = B[i]
        try:
            # Replace NaN/Null value with Infinity to keep comparisons moving.
            if b is None or b == None or np.isnan(b):
                b = float("inf")
        except:
            pass
         
        # Begin counting the number of occurrences of A.
        if a not in primary_A_counts:
            primary_A_counts[a] = 1
        else:
            primary_A_counts[a] += 1
  
        if a not in results:
            # a isn't already there, so add its first corresponding b value as dict '{b:1}'
            results[a] = {b:1}
        else:
            # a is already there
            if b not in results[a]:
                results[a][b] = 1
            else:
                results[a][b] += 1
    #print(results)
 
    # Set up counting dictionaries.
    a_to_most_common_b_count = {}
    a_to_all_bs_total = {}
 
    # Begin comparisons, find the most common counts of values between A and B.
    for a in results:
        bs = results[a]
        bs_list = list(bs.items())
        bs_list.sort(key = lambda x: x[1], reverse=True)
  
        b_val_most_common       = bs_list[0][0]
        b_val_most_common_count = bs_list[0][1]
  
        a_to_most_common_b_count[a] = b_val_most_common_count
        a_to_all_bs_totally_counted_sum = sum(bs.values())
        a_to_all_bs_total[a] = a_to_all_bs_totally_counted_sum
      
    fullcolumn_a_to_most_common_b_count_sum = sum(a_to_most_common_b_count.values())
    fullcolumn_a_to_all_bs_total            = sum(a_to_all_bs_total.values())
    fullcolumn_a_to_nonmost_common_b_count_sum = (fullcolumn_a_to_all_bs_total - fullcolumn_a_to_most_common_b_count_sum)
 
    # Compute values into support (Positive) and violation (Negative) results.
    dependency_support = fullcolumn_a_to_most_common_b_count_sum
    dependency_violations = fullcolumn_a_to_nonmost_common_b_count_sum
     
    # Do so in percents too.
    dependency_support_perc = round(100*dependency_support/float(fullcolumn_a_to_all_bs_total), 2)
    dependency_violations_perc = round(100*dependency_violations/float(fullcolumn_a_to_all_bs_total), 2)
     
    # Final Dependency strength of the comparison of A and B is the percentage of statistical support as calculated above.
    dependency_strength = dependency_support_perc
     
    return (dependency_strength, dependency_support, dependency_support_perc, dependency_violations, dependency_violations_perc)
     
def doesAdetermineB(A,B):
    # Cast dataFrame columns to lists and init result dict.
    A = A.tolist()
    B = B.tolist()
    results = {}
     
    # Begin the determining to populate results
    for (i,a) in enumerate(A):
        #print( i, a )
         
        try:
            # Replace NaN/Null value with Infinity to keep comparisons moving.
            if a is None or a == None or np.isnan(a):
                a = float("inf")
        except:
            pass
       
        # Get the corresponding value in B
        b = B[i]
        try:
            # Replace NaN/Null value with Infinity to keep comparisons moving.
            if b is None or b == None or np.isnan(b):
                b = float("inf")
        except:
            pass
  
        if a not in results:
            # a isn't already there, so add its
            # first corresponding b value as list '[b]'
            results[a] = set([b])
        else:
            results[a].add(b)
     
    # Now work through the results in A.
    for a in results:
        bs = results[a]
        # Convert the sets of bs to lists for easier length analysis
        results[a] = list(bs)
 
    # Just get the list of all the 'b' (right-side) lists
    all_bs = list(results.values())
     
    # Get the length of each of the 'b' lists
    all_bs_lengths = list( len(bs) for bs in all_bs )
    max_bs_length = max(all_bs_lengths)
     
    # Result Determination
    if max_bs_length == 1:
        A_determines_B = True
    else:
        A_determines_B = False
      
    return A_determines_B
 
def A_determines_B_strength_byIndex(dataFrame, column_index_A, column_index_B):
    # Get all Column names from the dataFrame.
    all_column_names = list(dataFrame.columns)
     
    # Get Columns to scan by Index.
    column_name_A = all_column_names[column_index_A]
    column_name_B = all_column_names[column_index_B]
     
    # Use those names to assign columns of data from the frame.
    colvals_A = dataFrame[column_name_A]
    colvals_B = dataFrame[column_name_B]
     
    # Run does A determine B's strength to get a result.
    return A_determines_B_strength(colvals_A, colvals_B)
 
###########################
## Data Identity Builder ##[DID_BUILD]
###########################
def buildDataIdentities(field_file="fields.json"):
    # Use Class Method to create library of data identities to compare against from json.
    return DataIdentity.generateIdentities(field_file)
 
def getDFnumRecords(df):
    return len(df.index)
 
def getDFnumDuplicatedRecords(df):
    try:
        return df.duplicated(keep='first').sum()
    except:
        return None
 
def getMinDatetimeStamp(df):
    """ df is the input dataframe
    """
    try:
        datetimes = pd.to_datetime(df['datetime.1'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.min(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.min(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['hour'], format='%Y%m%d%H')
        return pd.Timestamp.round(datetimes.min(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['eventtime'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.min(), freq='H')       
    except:
        pass
     
    return "NA. No known timestamp field."
     
def getMaxDatetimeStamp(df):
    """ df is the input dataframe
    """
    try:
        datetimes = pd.to_datetime(df['datetime.1'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.max(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.max(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['hour'], format='%Y%m%d%H')
        return pd.Timestamp.round(datetimes.max(), freq='H')
    except:
        pass
    try:
        datetimes = pd.to_datetime(df['eventtime'], format='%Y-%m-%d %H:%M:%S')
        return pd.Timestamp.round(datetimes.max(), freq='H')       
    except:
        pass
     
    return "NA. No known timestamp field."
 
 
#################################
### Data Config and Constants ###[DAT_CONFIG]
#################################
# Example Blank Column for creating new Columns
CONST_blankCol = {"name":"New Column", "type":"string", "nullable":False, "min_length":0,
                  "max_length":0, "functions":[], "children":[]}

# This constant is for building the dataframe which will hold the results of the analysis.
CONST_metadataframe = ["data_field_name","num_null_values","percent_null_values","num_unique_values","percent_unique_values",
                       "min_length_stringlike","max_length_stringlike","most_common_length_stringlike","mean_len_stringlike","stdev_length_stringlike",
                       "most_common_value","least_common_value","min_numeric","numeric_percentile_25","numeric_percentile_50","numeric_percentile_75","max_numeric",
                       "avg_numeric","stdev_numeric","top_five_values","bottom_five_values","multimodal_flag","near_zero_variance_flag",
                       "key_candidate_column","key_strength","key_support_count","key_violation_count","key_violation_perc",
                       "depends_determinant_cols","depends_dependent_cols","depends_strength","depends_supports","depends_perc","depends_violation_count","depends_violation_perc"]
 
# This constant is used as a stub for any tests not run.
# We'll use Numpy nan as the "not applicable". I had the string "N/A" before.
CONST_stub = np.nan
# Infer String Nulls in unknown data analysis.
CONFIG_inferNull = True
# String Nulls to be converted to None, if CONFIG_inferNull is True.
CONFIG_inferNullStrings = [np.nan,"","NULL","Null","null","(null)","NAN","NaN","Nan","nan","-","n/a",
                           "N/A","N/a","NA","Na","na","None","none","[]","NAT","NaT","Nat","nat","9.96921e+36",9.96921e+36]
# String Nulls are converted to a ...
CONFIG_inferNullReplace = None
# Columns full of Null in unknown data analysis are dropped.
CONFIG_dropNullCol = False
 
#######################
### Data Identifier ###[DAT_IDENT]
#######################
def dataIdentifier(data_frame,data_identities):
    # Takes a data frame, compares it against data supplied identities, then spits out a result based on five rudimentary tests.
    # Returns a dataframe with pass/fail data for each test.
     
    ## Setup ##
    ###########
    try:
        df      = data_frame        # Data Frame
        df_col  = df.columns.values.tolist()
        di      = data_identities   # Data Identities List
        re      = {}                # Counter Results
    except:
        raise "dataIdentifier: Error! Given 'dataframe' may not be a dataframe."
     
    for f in di:
        # Create preliminary counts to start at 0 each.
        countingDict(re,f.getName(),0)
     
    res_columns = ["Data Identity","Column Count","Strict Comparison","Lazy Comparison","Intersection","Key Fields"]
    all_results = pd.DataFrame(columns=res_columns)
     
    # Iterate through data identities against incoming data frame.
    ## Iteration ##
    ###############
    for id in di:
        # Checks to be checked against.
        colCount    = False # Column count
        strictCol   = False # Strict column comparison
        lazyCol     = False # Lazy column comparison
        inters      = False # Intersection check
        keys        = False # Key field check
         
        # Column Count
        if(id.field_count == len(df_col)):
            countingDict(re,id.getName(),1)
            colCount = True
         
        # Strict Column Comparison
        if(id.getFieldNames() == df_col):
            countingDict(re,id.getName(),1)
            strictCol = True
         
        # Lazy Column Comparison
        fs = sorted(id.getFieldNames())
        ss = sorted(df_col)
         
        if(fs == ss):
            countingDict(re,id.getName(),1)
            lazyCol = True
         
        # Set Intersection
        intersect = list(set(id.getFieldNames()).intersection(set(df_col)))
        if(len(intersect) == id.getFieldCount() and len(intersect) != 0):
            countingDict(re,id.getName(),1)
            inters = True
         
        # Key Field Check
        for fkf in id.getKeyFields():
            if(fkf in df_col):
                countingDict(re,id.getName(),1)
                keys = True
         
        test_results = {"Data Identity":id.getName(),
                        "Column Count":colCount,
                        "Strict Comparison":strictCol,
                        "Lazy Comparison":lazyCol,
                        "Intersection":inters,
                        "Key Fields":keys}
        all_results = all_results.append(test_results,ignore_index=True)
     
    ## Return ##
    ############
    all_results = all_results.set_index("Data Identity")
     
    return all_results,re

#####################
### Data Profiler ###[DAT_PROF]
#####################
def dataProfiler(data_frame,identify_results,data_identities,printout=False):
    # Interprets the results of the above function to determine what sort of data it is.
    # Then runs a battery of tests based on what it is identified as.
    # The results of which are then returned in a dataframe.
    ## Setup ##
    ###########
    df      = data_frame            # Data Frame
    ir      = identify_results      # Identity Data
    di      = data_identities       # Data Identity File Reference
    co      = None                  # Confirmed Data Identity.
     
    ## Result Check ##
    ##################
    for d in di:
        ind_columns = ["Column Count","Strict Comparison","Lazy Comparison","Intersection","Key Fields"]
        row = ir.loc[d.getName()]
        com = pd.Series(data=[True,True,True,True,True],name=d.getName(),index=ind_columns,dtype=object)
        lcom = pd.Series(data=[True,False,True,True,True],name=d.getName(),index=ind_columns,dtype=object)
         
        if(row.equals(com) or row.equals(lcom)):
            print("Identification has succeeded.\n")
            co = d
            break
        else:
            #print("Identification continues...")
            continue
     
    # Check if they did find a result.
    if(co != None):
        ## Identified as ... ##
        #######################
        print("Data Identified as "+str(co))
         
        # First find and replace all null strings with actual nulls.
        for ns in co.getNullString():
            df = df.replace({ns:None})
         
        # Create blank dataframe to work in.
        res_columns = CONST_metadataframe
        all_results = pd.DataFrame(columns=res_columns)
         
        # Run data profiling on that dataframe.
        fields_data = co.getColumnData()
        for fd in fields_data:
            col = fd.getName()
            work_col = df[col].tolist()          # Column Data, selected by name.
            work_ind = df.columns.get_loc(col)   # Column Index in dataframe, selected by name.
            funcs    = fd.getFunctions()         # Functions to be run.
             
            if("countUniqueValues" in funcs):
                # Perform count of unique values.
                results_unique  = countUniqueValues(work_col)
                if(printout):
                    print("Unique Values in "+col+": "+str(results_unique[0]))
                    print("Non-Unique Values in "+col+": "+str(results_unique[1]))
                    print("Instances of Non-Uniques past the First:\n"+str(results_unique[2]))
                    print("Unique Percentage: "+str(results_unique[3]))
            else:
                results_unique  = (0,0,0,0)
                
            if("countNullValues" in funcs):
                # Perform count of null values.
                results_nulls   = countNullValues(work_col)
                if(printout):
                    print("Nulls in "+col+": "+str(results_nulls[0])+"/"+str(len(work_col)))
                    print("Null Percentage: "+str(results_nulls[1]))
            else:
                results_nulls   = (0,0)
             
            if("countMaxMinLength" in funcs):
                # Perform count of maximum/minimum length.
                results_length  = countMaxMinLength(work_col)
                if(printout):
                    print("Min Length of "+col+": "+str(results_length[0]))
                    print("Max Length of "+col+": "+str(results_length[1]))
            else:
                results_length  = (0,0)
             
            if("countAverageLength" in funcs):
                # Figure out average length and standard deviation.
                results_avglen  = countAverageLength(work_col)
                if(printout):
                    print("Average Length of "+col+": "+str(results_avglen[0]))
                    print("Standard Deviation of Length in "+col+": "+str(results_avglen[1]))
            else:
                results_avglen  = (CONST_stub,CONST_stub)
             
            if("countAverageNumber" in funcs):
                # Figure out the average numeric value of column.
                results_avgnum  = countAverageNumber(work_col)
                if(printout):
                    print("Average Number of "+col+": "+str(results_avgnum))
            else:
                results_avgnum  = (CONST_stub)
                 
            if("countMaxMinValues" in funcs):
                # Count the alphanumerically largest and smallest values.
                results_maxmin  = countMaxMinValues(work_col,col)
                if(printout):
                    print("Lowest Entry in "+col+": "+str(results_maxmin[0]))
                    print("25th Percentile in "+col+": "+str(results_maxmin[2]))
                    print("50th Percentile in "+col+": "+str(results_maxmin[3]))
                    print("75th Percentile in "+col+": "+str(results_maxmin[4]))
                    print("Highest Entry in "+col+": "+str(results_maxmin[1]))
            else:
                results_maxmin  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub,CONST_stub)
             
            if("countCommonValues" in funcs):
                # Count the most common and least commonly occurring values.
                results_common  = countCommonValues(work_col)
                if(printout):
                    print("Least Common Occurrence in "+col+": "+str(results_common[0]))
                    print("Most Common Occurrence in "+col+": "+str(results_common[1]))
                    print("Bottom 5 Occurrences in "+col+": "+str(results_common[2]))
                    print("Top 5 Occurrences in "+col+": "+str(results_common[3]))
            else:
                results_common  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
             
            if("countStandardDeviation" in funcs):
                # Count numeric standard deviation of values.
                results_stdev   = countStandardDeviation(work_col)
                if(printout):
                    print("Standard Deviation of "+col+": "+str(results_stdev))
            else:
                results_stdev   = (CONST_stub)
             
            if("findDataTypes" in funcs):
                # Guess data types of values in column.
                results_dtypes  = findDataTypes(work_col)
                if(printout):
                    print("Data Types found in "+col+": "+str(results_dtypes))
            else:
                results_dtypes  = (CONST_stub)
             
            if("countCommonLength" in funcs):
                # Count the most common and least common lengths of values.
                results_comlen  = countCommonLength(work_col)
                if(printout):
                    print("Least Common Length in "+col+": "+str(results_comlen[0]))
                    print("Most Common Length in "+col+": "+str(results_comlen[1]))
                    print("Bottom 5 Lengths in "+col+": "+str(results_comlen[2]))
                    print("Top 5 Lengths in "+col+": "+str(results_comlen[3]))
            else:
                results_comlen  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
                 
            if("getNearZeroVariance" in funcs):
                # Figure out if the field has near-zero variance.
                results_n0vars  = getNearZeroVariance(work_col)
                if(printout):
                    print("Near Zero Variance Check: "+str(results_n0vars))
            else:
                results_n0vars  = (CONST_stub)
             
            if("getModality" in funcs):
                # Figure out modality of the field.
                results_modal   = getModality(work_col)
                #results_modal   = Multimodal_flag(work_col)
                if(printout):
                    print("Multimodal Flag: "+str(results_modal))
            else:
                results_modal   = (CONST_stub)
             
            if("getKeyCandidate" in funcs):
                # Figure out Key Candidacy
                results_keycan  = getKeyCandidate(work_col)
                if(printout):
                    print("Key Candidacy: "+str(results_keycan))
            else:
                results_keycan  = (CONST_stub)
             
            if("getKeyStrengths" in funcs):
                # Figure out key strength.
                results_keystr  = getKeyStrengths(work_col)
                if(printout):
                    print("Key Strength: "+str(results_keystr[0])+"%")
                    print("Key Support: "+str(results_keystr[1]))
                    print("Key Violations: "+str(results_keystr[2]))
                    print("Key Violation Percent: "+str(results_keystr[3])+"%")
            else:
                results_keystr  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
             
            if("getDeterminedByColumns" in funcs):
                # Get dependencies based on determinant columns.
                results_depbyc  = getDeterminedByColumns(df,work_ind)
                if(printout):
                    print("Determinant Columns: "+str(results_depbyc))
            else:
                results_depbyc  = (CONST_stub)
             
            if("getDeterminedColumns" in funcs):
                # Get dependencies based on dependent columns.
                results_detcol  = getDeterminedColumns(df,work_ind)
                if(printout):
                    print("Dependent Columns: "+str(results_detcol))
            else:
                results_detcol  = (CONST_stub)
             
            if("getDependencyStrengths" in funcs):
                results_depstr  = getDependencyStrengths(df,work_ind)
                if(printout):
                    print("Dependency Strength: "+str(results_depstr[0])+" / 100")
                    print("Dependency Support Count: "+str(results_depstr[1]))
                    print("Dependency Support Percentage: "+str(results_depstr[2])+"%")
                    print("Dependency Violation Count: "+str(results_depstr[3]))
                    print("Dependency Violation Percentage: "+str(results_depstr[4])+"%")
            else:
                results_depstr  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub,CONST_stub)
             
            # Code stub for future functions to be added.
            '''
            if("get" in funcs):
                pass
            else:
                pass
            '''
             
            ## Compile Results ##
            #####################
            # Here comes the part where the code fucktouples in size thanks to my incompetence.
            result_row   = {
                "data_field_name":                  col,
                ###"data_field":                       col,
                 
                "num_unique_values":                results_unique[0],
                "percent_unique_values":            results_unique[3],
                 
                "num_null_values":                  results_nulls[0],
                "percent_null_values":              results_nulls[1],
                 
                "min_length_stringlike":            results_length[0],
                "max_length_stringlike":            results_length[1],
                 
                "most_common_length_stringlike":    results_comlen[1],
                 
                "mean_len_stringlike":              results_avglen[0],
                "stdev_length_stringlike":          results_avglen[1],
                 
                "least_common_value":               results_common[0],
                "most_common_value":                results_common[1],
                 
                "min_numeric":                      results_maxmin[0],
                "numeric_percentile_25":            results_maxmin[2],
                "numeric_percentile_50":            results_maxmin[3],
                "numeric_percentile_75":            results_maxmin[4],
                "max_numeric":                      results_maxmin[1],
                 
                "avg_numeric":                      results_avgnum,
                "stdev_numeric":                    results_stdev,
                 
                "top_five_values":                  results_common[3],
                "bottom_five_values":               results_common[2],
                 
                "multimodal_flag":                  results_modal,
                "near_zero_variance_flag":          results_n0vars,
                 
                "key_candidate_column":             results_keycan,
                "key_strength":                     results_keystr[0],
                "key_support_count":                results_keystr[1],
                "key_violation_count":              results_keystr[2],
                "key_violation_perc":               results_keystr[3],
                 
                "depends_determinant_cols":         results_depbyc,
                "depends_dependent_cols":           results_detcol,
                "depends_strength":                 results_depstr[0],
                "depends_supports":                 results_depstr[1],
                "depends_perc":                     results_depstr[2],
                "depends_violation_count":          results_depstr[3],
                "depends_violation_perc":           results_depstr[4]
            }
            all_results = all_results.append(result_row,ignore_index=True)
         
        ## Returning ##
        ###############
        # Set an index and return results.
        #all_results = all_results.set_index("data_field_name")
        #return all_results
        DP = all_results
    else:
        ## Not Identified ##
        ####################
        print("Data Not Identified.\n")
        DP = unknownDataProfiler(data_frame,printout)
     
    for profile_field_name in DP.columns:
        DP[profile_field_name] = DP[profile_field_name].astype(str)
     
    return DP
     
 
#############################
### Unknown Data Profiler ###[DAT_UPROF]
#############################
def unknownDataProfiler(data_frame,printout,infer_null=CONFIG_inferNull):
    # Data hasn't been identified. Do an in-depth, inferred analysis.
    df = data_frame
    pr = printout
    
    # Temporarily suppress Numpy warnings.
    # Old settings get returned to variable to reuse later.
    errset = np.seterr(divide="ignore")
    
    print("Starting Unknown Data Profiling...")
     
    # Create dataframe to contain findings information.
    res_columns = CONST_metadataframe
    all_results = pd.DataFrame(columns=res_columns)
     
    # Remove inferred string nulls.
    if(infer_null):
        for ns in CONFIG_inferNullStrings:
            df = df.replace({ns:CONFIG_inferNullReplace})
#    else:
#        df = df.replace({"":None})
     
    for col in df.columns:       
        print("Now profiling column: " + col)
       
        work_col = df[col].tolist()
        work_ind = df.columns.get_loc(col)  # Column Index in dataframe, selected by name.
 
        #curr_col = df[col]
        #curr_col = curr_col.astype("float64")
        #work_col = curr_col.tolist()
         
        #print(curr_col)
        #print()
        #print(work_col)
        #print()
         
        # Check if all column contents are numeric.
        numeric_check = [x for x in work_col if isinstance(x,(int,float))]
        string_check  = [x for x in work_col if isinstance(x,(str))]
        null_check    = work_col.count(None) + work_col.count(np.nan)
        numeric       = False
        stringy       = False # If I use 'string' I'll overwrite something very important.  :V
        mixed         = False
        nothing       = False
         
        
        '''
        print("## "+col+" : Total Recs: "+str(len(work_col))+
              " | Numeric: "+str(len(numeric_check))+
              " | String: "+str(len(string_check))+
              " | Null: "+str(null_check))
        '''
        
         
        work_length = len(work_col)
         
        # Compare check results with the main
        if(len(numeric_check) > 0 and len(numeric_check)+null_check == work_length):
            # Data is numeric.
            numeric = True
        elif(len(string_check) > 0 and len(string_check)+null_check == work_length):
            # Data is strings.
            stringy = True
        elif(null_check != work_length):
            # Data is both(?)
            mixed   = True
        else:
            nothing = True
         
        #print(str([numeric,stringy,mixed,nothing]))
         
        if(nothing == True and CONFIG_dropNullCol):
            # Column is full of null values, and the option to drop them
            #print("Dropping Column "+col)
            pass
         
        if(pr):
            # Column Name
            print("\n # "+col+" #")
         
        # Begin doing general tests. No stubs needed for these since they're always run.
        if(nothing == False):
            results_unique  = countUniqueValues(work_col)
            results_nulls   = countNullValues(work_col)
            results_length  = countMaxMinLength(work_col)
            results_common  = countCommonValues(work_col)
            results_n0vars  = getNearZeroVariance(work_col)
             
            results_keycan  = getKeyCandidate(work_col)
            results_keystr  = getKeyStrengths(work_col)
             
            results_depbyc  = getDeterminedByColumns(df,work_ind)
            results_detcol  = getDeterminedColumns(df,work_ind)
            results_depstr  = getDependencyStrengths(df,work_ind)
        else:
            # Unless, of course, teh whole column is null...
            results_unique  = (0,0,0,0)
            results_nulls   = (0,0)
            results_length  = (0,0)
            results_common  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
            results_n0vars  = (CONST_stub)
             
            results_keycan  = (CONST_stub)
            results_keystr  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
             
            results_depbyc  = (CONST_stub)
            results_detcol  = (CONST_stub)
            results_depstr  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub,CONST_stub)
         
        # Begin next round of tests.
        if(numeric):
            # Run numeric tests
            results_avgnum  = countAverageNumber(work_col)
            results_stdev   = countStandardDeviation(work_col)
            results_maxmin  = countMaxMinValues(work_col,col)
            results_modal   = getModality(work_col)
        if(stringy):
            # Run non-numeric tests
            results_avglen  = countAverageLength(work_col)
            results_comlen  = countCommonLength(work_col)
        if(mixed):
            # Run both, but not Modality
            results_avgnum  = countAverageNumber(work_col)
            results_stdev   = countStandardDeviation(work_col)
            results_maxmin  = countMaxMinValues(work_col,col)
            results_modal   = (CONST_stub)
            results_avglen  = countAverageLength(work_col)
            results_comlen  = countCommonLength(work_col)
         
        # Put stubs where tests weren't run.
        # If mixed, no stubs needed.
        if(numeric or nothing):
            # String Functions get stubs.
            results_avglen  = (0,0)
            results_comlen  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub)
            try: results_avglen  = countAverageLength(work_col)
            except: pass
            try: results_comlen  = countCommonLength(work_col)
            except: pass
        if(stringy or nothing):
            # Number functions get stubs.
            results_avgnum  = (0)
            results_stdev   = (CONST_stub)
            results_maxmin  = (CONST_stub,CONST_stub,CONST_stub,CONST_stub,CONST_stub)
            results_modal   = (CONST_stub)
            try: results_avgnum  = countAverageNumber(work_col)
            except: pass
            try: results_stdev   = countStandardDeviation(work_col)
            except: pass
            try: results_maxmin  = countMaxMinValues(work_col,col)
            except: pass
            try: results_modal   = getModality(work_col)
            except: pass
             
        # If printout's enabled, write results to console.
        if(pr):
            # Unique Values
            print("Unique Values in "+col+": "+str(results_unique[0]))
            print("Non-Unique Values in "+col+": "+str(results_unique[1]))
            print("Instances of Non-Uniques past the First:\n"+str(results_unique[2]))
            print("Unique Percentage: "+str(results_unique[3]))
             
            # Null Values
            print("Nulls in "+col+": "+str(results_nulls[0])+"/"+str(len(work_col)))
            print("Null Percentage: "+str(results_nulls[1])+"\n")
             
            # Length Values
            print("Min Length of "+col+": "+str(results_length[0]))
            print("Max Length of "+col+": "+str(results_length[1]))
             
            # Common Values
            print("Least Common Occurrence in "+col+": "+str(results_common[0]))
            print("Most Common Occurrence in "+col+": "+str(results_common[1]))
            print("Bottom 5 Occurrences in "+col+": "+str(results_common[2]))
            print("Top 5 Occurrences in "+col+": "+str(results_common[3])+"\n")
            
            # Average Length and Standard Deviation
            print("Average Length of "+col+": "+str(results_avglen[0]))
            print("Standard Deviation of Length in "+col+": "+str(results_avglen[1]))
            
            print("Least Common Length in "+col+": "+str(results_comlen[0]))
            print("Most Common Length in "+col+": "+str(results_comlen[1]))
            print("Bottom 5 Lengths in "+col+": "+str(results_comlen[2]))
            print("Top 5 Lengths in "+col+": "+str(results_comlen[3])+"\n")
             
            # Average Number and Standard Deviation
            print("Average Number of "+col+": "+str(results_avgnum))
            print("Standard Deviation of "+col+": "+str(results_stdev))
             
            # Highest and Lowest Entries
            print("Lowest Entry in "+col+": "+str(results_maxmin[0]))
            print("25th Percentile in "+col+": "+str(results_maxmin[2]))
            print("50th Percentile in "+col+": "+str(results_maxmin[3]))
            print("75th Percentile in "+col+": "+str(results_maxmin[4]))
            print("Highest Entry in "+col+": "+str(results_maxmin[1])+"\n")
             
            # Multimodal Flag
            print("Multimodal Flag: "+str(results_modal))
             
            # Near Zero Variance
            print("Near Zero Variance Check: "+str(results_n0vars)+"\n")
             
            # Key Candidacy Check
            print("Key Candidacy: "+str(results_keycan))
             
            # Key Strength
            print("Key Strength: "+str(results_keystr[0])+"%")
            print("Key Support: "+str(results_keystr[1]))
            print("Key Violations: "+str(results_keystr[2]))
            print("Key Violation Percent: "+str(results_keystr[3])+"%"+"\n")
             
            # Determinite Columns
            print("Determinant Columns: "+str(results_depbyc))
             
            # Dependent Columns
            print("Dependent Columns: "+str(results_detcol))
             
            # Dependency Strength
            print("Dependency Strength: "+str(results_depstr[0])+" / 100")
            print("Dependency Support Count: "+str(results_depstr[1]))
            print("Dependency Support Percentage: "+str(results_depstr[2])+"%")
            print("Dependency Violation Count: "+str(results_depstr[3]))
            print("Dependency Violation Percentage: "+str(results_depstr[4])+"%")
 
             
 
        ##return (len(uniques),len(non_uniques),non_uniques,round((len(uniques)/float(len(colList)))*100,2), uniques, sum(non_uniques.values()))
        '''
        try:
            print('NumUniques: ', col, results_unique[0], results_unique[3], results_unique[5])
        except:
            print(results_unique)
        '''
         
        # Write results to a dict.
        result_row   = {
            "data_field_name":                  col,
             
            "num_unique_values":                results_unique[0],
            "percent_unique_values":            results_unique[3],
             
            "num_null_values":                  results_nulls[0],
            "percent_null_values":              results_nulls[1],
             
            "min_length_stringlike":            results_length[0],
            "max_length_stringlike":            results_length[1],
             
            "most_common_length_stringlike":    results_comlen[1],
             
            "mean_len_stringlike":              results_avglen[0],
            "stdev_length_stringlike":          results_avglen[1],
             
            "least_common_value":               results_common[0],
            "most_common_value":                results_common[1],
             
            "min_numeric":                      results_maxmin[0],
            "numeric_percentile_25":            results_maxmin[2],
            "numeric_percentile_50":            results_maxmin[3],
            "numeric_percentile_75":            results_maxmin[4],
            "max_numeric":                      results_maxmin[1],
             
            "avg_numeric":                      results_avgnum,
            "stdev_numeric":                    results_stdev,
             
            "top_five_values":                  results_common[3],
            "bottom_five_values":               results_common[2],
             
            "multimodal_flag":                  results_modal,
            "near_zero_variance_flag":          results_n0vars,
             
            "key_candidate_column":             results_keycan,
            "key_strength":                     results_keystr[0],
            "key_support_count":                results_keystr[1],
            "key_violation_count":              results_keystr[2],
            "key_violation_perc":               results_keystr[3],
             
            "depends_determinant_cols":         results_depbyc,
            "depends_dependent_cols":           results_detcol,
            "depends_strength":                 results_depstr[0],
            "depends_supports":                 results_depstr[1],
            "depends_perc":                     results_depstr[2],
            "depends_violation_count":          results_depstr[3],
            "depends_violation_perc":           results_depstr[4]
        }
        try: all_results = all_results.append(result_row,ignore_index=True)
        except OverflowError: 
            print("OverFlow with:")
            print(result_row)

    # Set result dataframe index to column name.
    #all_results = all_results.set_index("data_field_name")
    
    # Reset Numpy Warnings
    np.seterr(**errset)
    
    # Return results.
    return all_results


###############
### Outputs ###[OUTPUT]
###############
def getHTMLHeader(inputName):
    # Dynamically generate some HTML header data.
    return """<html>
<head>
    <meta charset="utf-8" />
    <title>Data Identification/Profiling Results | """+getTimeStr()+"""</title>
    <meta author='Luca Pavone'/>
    <meta created='"""+getTimeStr()+"""'/>
</head>
<body>
"""
 
def getHTMLBody(df_info,di_html,dp_html,df,inputName):
    # Dynamically generate some HTML body data, by slotting HTML dataframes in.
    return """<h1>Data Identity/Profile Results</h1>
<p>This page was generated on """+getTimeStr()+""".</p>
<h3> Input File was: """+inputName+""" </h3>
<h2>Dataframe Information</h2>
<ul>"""+df_info+"""
</ul>
 
<b>Number of Records: </b>"""+str(getDFnumRecords(df))+"""<br>
<b>Num. Duplicate Records: </b>"""+str(getDFnumDuplicatedRecords(df))+"""<br>
<b>Profile Start Time: </b>"""+str(getMinDatetimeStamp(df))+"""<br>
<b>Profile End Time: </b>"""+str(getMaxDatetimeStamp(df))+"""<br>
 
 
<h2>Identification Results</h2>
"""+di_html+"""
 
<h2>Profiling Results</h2>
"""+dp_html+"""
"""
def getHTMLFooter():
    # Dynamically generate some HTML footer data.
    return """</body>
</html>"""
 
def getTimeStr(timeForm='%B %d %Y - %H:%M:%S'):
    # Gets the current time, returns as a string.
    time = dt.now().strftime(timeForm)
    return str(time)
 
def createHTML(df,inputName,di,dp,filepath="./html_file",fileext=".html"):
    # Simple 'Create' HTML from Dataframe.
    # Takes the Identify and Profile Result Dataframes,
    # and exports them into a HTML file.
     
    pd.set_option('display.max_colwidth', 250)
 
    # Set up containers, one for containing our facts,
    # one to hold the facts translated into HTML
    df_info   = {}
    df_string = ""
     
    # Gather data about Dataframe
    df_info["rows"] = str(df.shape[0])
    df_info["columns"] = str(df.shape[1])
    df_info["size"] = str(df.size) # Columns * Rows
  
    # Step through dictionary to generate HTML string.
    for (k,v) in df_info.items():
        pass
        df_string += "<li><strong>"+k.title()+".</strong> "+str(v)+"</li>"
     
    # Take results tables, export to HTML
    di_html = di.to_html()
    dp_html = dp.to_html()
     
    # Open file to write.
    path    = filepath+fileext
    with open(path,"w") as outfile:
        # Compile content to write to file.
        html = getHTMLHeader(inputName)+getHTMLBody(df_string,di_html,dp_html,df,inputName)+getHTMLFooter()
         
        # Write it to file.
        outfile.write(html)
        print("createHTML: Success!")
 
#%%
#display(dataF)
 
'''DI = buildDataIdentities()
DDI = dataIdentifier(dataF,DI)
#display(DDI)
DP = dataProfiler(dataF,DDI[0],DI,printout=False)
createHTML(dataF,DDI[0],DP)'''
 
 
def main():
    #root = tk.Tk()
    #root.title("Select CSV file")
    #w=350
    #h=100
    #sw = root.winfo_screenwidth()
    #sh = root.winfo_screenheight()
    #x = int(sw/2-w/2)
    #y = int(sh/2-h/2)
    #root.geometry(str(w) + 'x' + str(h) + '+' + str(x) + '+' + str(y))
     
    window = MetaFrame()
     
    window.gendata()
     
    #window.mainloop()
    #try:
    #    root.destroy()
    #except tk.TclError:
    #    pass
     
main()