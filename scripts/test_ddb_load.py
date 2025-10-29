## Looking at stuff
import pandas as pd; import numpy as np;
config_file = pd.read_csv("c:/users/alanj/Dropbox/py/ptc/test_src/c0.psv", sep="|", header=None, on_bad_lines='warn', na_values=['']);
config_file.columns = ['dc','mtype','col','coltype','placerow'];
import duckdb; import time;
con = duckdb.connect();
fileDir = 'c:/users/alanj/Dropbox/py/ptc/test_data'; file_use = 'd0_1000000.csv'
write_n = 100000; kt = 'D';  use_config = config_file[config_file['dc'] == kt].reset_index(inplace=False, drop=True);
saveDir = 'c:/users/alanj/Dropbox/py/ptc/save_data'
all_mtypes = list(set(use_config['mtype'].to_list()))
k = 3;
myFT = use_config[use_config['mtype'] == all_mtypes[k]].reset_index(inplace=False, drop=True);

duckdbTypeMap= {"c1":"VARCHAR","i64":"BIGINT","i32":"INTEGER","d102":"DECIMAL(10, 2)","D102":"DECIMAL(10,2)","d142":"DECIMAL(14,2)",
                "f64":"FLOAT","sym":"VARCHAR","str":"VARCHAR","STR":"VARCHAR","D185":"DECIMAL(18,5)","d185":"DECIMAL(18,5)",
                "TNS":"BIGINT", "tns":"BIGINT","tus":"BIGINT","TUS":"BIGINT","Tus":"BIGINT","Tns":"BIGINT","dlt64":"BIGINT","DLT64":"BIGINT"}
ddbTypes = myFT['coltype'].map(duckdbTypeMap)
MyStr = ("SET threads TO 8;CREATE OR REPLACE TABLE t_tab as SELECT * FROM read_csv('" + fileDir + "/" + file_use + "', header=false,\n" + 
          "skip = 2,delim=',', strict_mode = false, ignore_errors=True,null_padding=True,auto_detect = false,\n" + 
          #" names = ['" + "','".join(myFT['col'].to_list()) + "'],\n" + 
          "columns = {" + ",".join(["'" +  myFT['col'][ix] + "':'" + myFT['coltype'].map(duckdbTypeMap)[ix] + "'" for ix in range(len(myFT))]) + 
          ",'orig_i':'BIGINT'})\n" +
          "where mtype='T';");

time_00 = time.time();
con.execute(MyStr)
con.sql("summarize t_Tab").show();
time_01 = time.time();
print("Time to execute t_tab loader was " + str(time_01-time_00));
t_00 = time.time();
for k in range(len(all_mtypes)) :
  myFT = use_config[use_config['mtype'] == all_mtypes[k]].reset_index(inplace=False, drop=True)
  print("load k = " + str(k) + "," + all_mtypes[k])
  MyStr = ("SET threads TO 8;COPY (SELECT * FROM read_csv('" + fileDir + "/" + file_use + "', header=false,\n" + 
          "skip = 2,delim=',', strict_mode = false, ignore_errors=True,null_padding=True,auto_detect = false,\n" + 
          #" names = ['" + "','".join(myFT['col'].to_list()) + "'],\n" + 
          "columns = {" + ",".join(["'" +  myFT['col'][ix] + "':'" + myFT['coltype'].map(duckdbTypeMap)[ix] + "'" for ix in range(len(myFT))]) + 
          ",'orig_i':'BIGINT'})\n" +
          "where mtype='" + all_mtypes[k] + "') to '" + saveDir + "/test_" + all_mtypes[k] + "_1000000.parquet' (format PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 19);");
  con.execute(MyStr)
t_01 = time.time();
print("Time to execute t_tab loader was " + str(t_01-t_00));

duckdb.sql("create or replace Table x_load as select * from read_parquet('" + saveDir + "/test_M_1000000.parquet')");
#SELECT *
#FROM read_csv(
#    'your_file.csv',
#    header = false,
#    names = ['id', 'name', 'age'],
#    columns = {'id': 'INTEGER', 'name': 'VARCHAR', 'age': 'INTEGER'}
#)
COPY
#SELECT *
#FROM read_csv(
#    'your_file.csv',
#    header = false,
#    names = ['id', 'name', 'age'],
#    columns = {'id': 'INTEGER', 'name': 'VARCHAR', 'age': 'INTEGER'}
#)
#WHERE age BETWEEN 20 AND 40;

fakempids = ['B','N','F','S','m','X'];
fakesrcs = ['B','N','I','X'];
fakesyms = ['aapl','btc','ibm','intel','stuff'];
fakestrs = ['','gd','ftx'];

rand_types = np.random.choice(np.unique(use_config['mtype']), size=write_n);
rand_prices = np.floor(np.random.uniform(0,20000, write_n)).astype(np.int64);
rand_prices = [(str(x)[0:(len(str(x))-2)] + '.' + str(x)[(len(str(x))-2):len(str(x))]) for x in rand_prices];
rand_qty = np.random.uniform(0,600, write_n);
rand_qty =  [str(x) for x in np.floor(rand_qty*100) / 100];
rand_oids = np.random.randint(low=1000000, high=9999999, size=write_n, dtype='l')
rand_old_oids = np.random.randint(low=1000000, high=9999999, size=write_n, dtype='l')
rand_times = np.random.randint(low=100,high=440320,size=write_n, dtype='l');
rand_dlts = np.random.randint(low=-50,high=230,size=write_n,dtype='l');
rand_dlts[(rand_dlts<=3) & (rand_dlts >= -3) ] = 0;
rand_dlts = [str(x) if x != 0 else "" for x in rand_dlts]
rand_srcs = np.random.choice(fakesrcs,size=write_n);
rand_mpids = np.random.choice(fakempids,size=write_n);
rand_str = np.random.choice(fakestrs,size=write_n); 

MyLines=[];
for ii in range(write_n) :
  if ii % 10000 == 0 :
    print("On ii = " + str(ii) + "/" + str(write_n))
  FT = use_config[use_config['mtype'] == rand_types[ii]].reset_index(inplace=False, drop=True);
  nt = [];
  for jj in range(len(FT)) :
    if (FT.iloc[jj].col == 'mtype') :
      nt.append(rand_types[ii])
    elif (FT.iloc[jj].col in ['mtime','time','rtime']) :
      nt.append(rand_times[ii]);  ## Note no matter what "read type target is", CSV is an i64 in micro-seconds.  Arrow will convert to timestamp real
    elif (FT.iloc[jj].col in ['dlts','dlt','delta']) :
      nt.append(rand_dlts[ii]);
    elif (FT.iloc[jj].col in ['oid','oids']) :
      nt.append(rand_oids[ii]);
    elif (FT.iloc[jj].col in ['oldoids','oldoid','oldid','oldis']) :
      nt.append(rand_old_oids[ii]);
    elif (FT.iloc[jj].col in ['src','srcs']) :
      nt.append(rand_srcs[ii]);
    elif (FT.iloc[jj].col in ['mpid','bmpid','mpids','smpie']) :
      nt.append(rand_mpids[ii]);
    elif (FT.iloc[jj].col in ['qty','quantity','Qty']) :
      nt.append(rand_qty[ii]);
    elif (FT.iloc[jj].col in ['price','bprice','sprices','prices']) :
      nt.append(rand_prices[ii]);
    else :
      nt.append(np.random.choice(fakestrs,size=1)[0]);
  MyLines.append(",".join([str(x) for x in nt])); 
  if len(MyLines) >= 20000 :
    MyText = "\n".join(MyLines)
    dirwrite = 'c:/users/alanj/Dropbox/py/ptc/test_data';
    import os;
    os.makedirs(dirwrite,exist_ok=True);
    with open(dirwrite+'/c0_' + str(write_n) + '.csv','at') as f :
      f.write(MyText);
    MyLines=[];
