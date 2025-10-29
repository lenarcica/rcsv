import pyarrow as pa
import pyarrow.ipc as ipc; import pandas as pd; import numpy as np;
from decimal import Decimal;

schema_dir = "c:/users/alanj/Dropbox/py/ptc/test_src";  
schema_file = "/c0.psv";
all_schemas = pd.read_csv(schema_dir + "/" + schema_file, sep="|", header=None, on_bad_lines='warn', na_values=['']);
all_schemas.columns = ['dc','mtype','col','coltype','placerow'];

data_dir = 'c:/users/alanj/Dropbox/py/ptc/test_data'
data_file = 'c0_0.csv'

save_dir = 'c:/users/alanj/Dropbox/py/ptc/save_data'

targetDC = 'D'; targetMtype = 'N'; 
target_schema = all_schemas[(all_schemas.dc == targetDC) & (all_schemas.mtype == targetMtype)].reset_index(inplace=False, drop=True);

readN = 10;
dM = {'i64':pa.int64(),'i32':pa.int32(),'I64':pa.int64(), 'D102':pa.decimal64(10, 2), 'D142':pa.decimal64(14,2),
      'd102':pa.decimal64(10,2),'d142':pa.decimal64(14,2), 'd185':pa.decimal64(18,5),'D185':pa.decimal64(18,5),
      'dlt':pa.int64(),'dlt64':pa.int64(),'DLT':pa.int64(),'DLT64':pa.int64(),'U64':pa.uint64(),u64:pa.uint64(),
      'N':None, 'C1':pa.binary(1), 'c1':pa.binary(1), 'C2':pa.binary(2),'c2':pa.binary(2),'str':pa.string(), 'Str':pa.string(),
      'F':pa.float64(),'F64':pa.float64(),'F32':pa.float32(), 'TNS': pa.timestamp('ns'), 'Tns':pa.timestamp('ns'), 'tns':pa.timestamp('ns'),
       'Tus':pa.timestamp('us'), 'TUS':pa.timestamp('us')}
def construct_schema(in_schema) :
  outlist = [];
  keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)];
  for ii in range(len(keep_schema)) :
    outlist.append(pa.field(keep_schema.col[ii], dM[keep_schema.coltype[ii]]))
  return(pa.schema(outlist));

def int64_oh_decima(in_str,idec) :
   ip = in_str.split('.');
   return( np.int64(ip[0]+'00' if (len(ip)<=1) else ( (ip[0] +ip[1] + '0'*(2-len(ip[1]))) if (len(ip[1])<2) else (ip[0] + ip[1][0:2]) ) ) );

input_array = pa.array(np.array([100,200,302,432,5343],dtype=np.int64),pa.int64());
c_array = input_array.cast(pa.decimal128(18,2))

import pyarrow as pa
# Example: Casting an int64 to decimal64(10, 2)
#int_array = pa.array([12345, 67890], type=pa.int64())
#decimal_type = pa.decimal128(10, 2) # Use decimal128 for more precision if needed
#decimal_array = int_array.cast(decimal_type)

convertable = pa.array(np.array([100,200,302,432,5343],dtype=np.int64),pa.decimal64(10,2))
def construct_arrow_reader(in_schema, readN) :
  outlist = [];
  in_schema['origrow'] = np.arange(len(in_schema));
  keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)].reset_index(inplace=False, drop=True);
  keep_schema = keep_schema.sort_values("placerow").reset_index(inplace=False, drop=True);
  for ii in range(len(keep_schema)) :
    if keep_schema.coltype[ii] in ['i64','I64'] :
      outlist.append(np.zeros(readN, np.int64))
    elif keep_schema.coltype[ii] in ['dlt64','DLT64','dlt','DLT'] :
      outlist.append(np.zeros(readN, np.int64))
    elif keep_schema.coltype[ii] in ['u64','U64']:
      outlist.append(np.zeros(readN, np.uint64))
    elif keep_schema.coltype[ii] in ['c2','C2'] :
      outlist.append(np.repeat('  ',readN).astype('|S2'))
    elif keep_schema.coltype[ii]  in ['c1','C1'] :
      outlist.append(np.repeat(' ',readN).astype('|S1'))
    elif keep_schema.coltype[ii] in ['d102','D102'] :
      outlist.append([Decimal('0') for x in range(readN)])
    elif keep_schema.coltype[ii] in ['D142','d142'] :
      outlist.append([Decimal('0') for x in range(readN)])
    elif keep_schema.coltype[ii] in ['D185','d185'] :
      outlist.append([Decimal('0') for x in range(readN)])
    elif keep_schema.coltype[ii] in ['tns','TNS','Tns'] :
      outlist.append(np.repeat(0,readN).astype(np.int64))
    elif keep_schema.coltype[ii] in ['tus','TUS','Tus'] :
      outlist.append(np.repeat(0,readN).astype(np.int64))
    elif keep_schema.coltype[ii] in ['f64','F64'] :
      outlist.append(np.zeros(readN, np.float64), pa.float64())
    elif keep_schema.coltype[ii] in ['str','String','Str','STR'] :
      outlist.append(np.repeat('          ', readN))
  return(outlist);


def table_cols_from_vecs(inlist, in_schema) :
  outlist = [];
  in_schema['origrow'] = np.arange(len(in_schema));
  keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)].reset_index(inplace=False, drop=True);
  keep_schema = keep_schema.sort_values("placerow").reset_index(inplace=False, drop=True);
  for ii in range(len(keep_schema)) :
    if keep_schema.coltype[ii] in ['i64','I64'] :
      outlist.append(pa.array(inlist[ii],pa.int64()));
    elif keep_schema.coltype[ii] in ['dlt64','DLT64','DLT','dlt'] :
      outlist.append(pa.array(inlist[ii],pa.int64()));
    elif keep_schema.coltype[ii] in ['u64','U64'] :
      outlist.append(pa.array(inlist[ii],pa.uint64()));
    elif keep_schema.coltype[ii] in ['c2','C2'] :
      outlist.append(pa.array(inlist[ii],pa.binary(2)));
    elif keep_schema.coltype[ii]  in ['c1','C1'] :
      outlist.append(pa.array(inlist[ii],pa.binary(1)));
    elif keep_schema.coltype[ii] in ['d102','D102'] :
      try :
        outlist.append(pa.array(inlist[ii],pa.decimal64(10,2)));
      except :
        print("table_cols_from_vecs, issue happened on ii = " + str(ii));
        breakpoint();
    elif keep_schema.coltype[ii] in ['D142','D142'] :
      try :
        outlist.append(pa.array(inlist[ii],pa.decimal64(14,2)));
      except :
        print("table_cols_from_vecs error on ii = " + str(ii) + " 142 type");
        breakpoint();
    elif keep_schema.coltype[ii] in ['d185','D185'] :
        outlist.append(pa.array(inlist[ii],pa.decimal64(18,5)));
    elif keep_schema.coltype[ii] in ['tns','TNS','Tns'] :
      outlist.append(pa.array(inlist[ii],pa.timestamp('ns')));
    elif keep_schema.coltype[ii] in ['tus','TUS','Tus'] :
      outlist.append(pa.array(inlist[ii],pa.timestamp('us')));
    elif keep_schema.coltype[ii] in ['f64','F64'] :
      outlist.append(pa.array(inlist[ii],pa.float64()));
    elif keep_schema.coltype[ii] in ['str','String','Str','STR'] :
      outlist.append(pa.array(inlist[ii],pa.string()));
  return(pa.schema(outlist));

def construct_blank_arrow_read(in_schema, readN) :
  outlist = [];
  keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)].reset_index(inplace=Falase,drop=True);
def construct_blank_arrow_read(in_schema, readN) :
  outlist = [];
  keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)].reset_index(inplace=Falase,drop=True);
  for ii in range(len(keep_schema)) :
    if keep_schema.coltype[ii] in ['i64','I64'] :
      outlist.append(pa.array(np.zeros(readN, np.int64), pa.int64()))
    elif keep_schema.coltype[ii] in ['c2','C2'] :
      outlist.append(pa.array(np.repeat('  ',readN).astype('|S2'), pa.binary(2)))
    elif keep_schema.coltype[ii] in ['c1','C1'] :
      outlist.append(pa.array(np.repeat('',readN).astype('|S1'), pa.binary(1)))
    elif keep_schema.coltype[ii] in ['d185','D185'] :
      outlist.append(pa.array(np.repeat(0,readN).astype(np.int64), pa.decimal64(18,5)))
    elif keep_schema.coltype[ii] in ['d102','D102'] :
      outlist.append(pa.array(np.repeat(0,readN).astype(np.int64), pa.decimal64(10,2)))
    elif keep_schema.coltype[ii] in ['D142','D142'] :
      outlist.append(pa.array(np.repeat(0,readN).astype(np.int64), pa.decimal64(14,2)))
    elif keep_schema.coltype[ii] in ['tns','TNS','Tns'] :
      outlist.append(pa.array(np.repeat(0,readN).astype(np.int64), pa.timestamp('ns')))
    elif keep_schema.coltype[ii] in ['tus','TUS','Tus'] :
      outlist.append(pa.array(np.repeat(0,readN).astype(np.int64), pa.timestamp('ns')))
    elif keep_schema.coltype[ii] in ['f64','F64'] :
      outlist.append(pa.array(np.zeros(readN, np.float64), pa.float64()))
    elif keep_schema.coltype[ii] in ['str','String','Str','STR'] :
      outlist.append(pa.array(np.repeat('          ', readN), pa.string()))
  return(outlist)
areader = construct_arrow_reader(target_schema,readN);
aschema = construct_schema(target_schema);

tgDate = '2025-01-12';
dt_int = np.asarray([pd.to_datetime(tgDate)],dtype='datetime64[ns]').astype(np.int64)[0];

in_schema = target_schema.copy();
in_schema['origrow'] = np.arange(len(in_schema));
keep_schema = in_schema[(in_schema.coltype != 'N') & (in_schema.placerow >= 0)].reset_index(inplace=False, drop=True);
keep_schema = keep_schema.sort_values("placerow").reset_index(inplace=False, drop=True);

with open(data_dir + '/' + data_file, "rt") as file:
  iline = 0; ipline = 0;
  for line in file: 
    if line[0] == targetMtype :
      LS = line.replace('\n','').split(',');
      for jj in range(len(LS)) :
        tctype = in_schema.coltype[jj];
        prj = in_schema.placerow[jj];
        if prj >= 0 : 
           if tctype in ['I64','i64'] :
             (areader[prj])[ipline] = np.int64(LS[jj])
           elif tctype in ['dlt','DLT64','DLT','DLT64'] :
             (areader[prj])[ipline] = 0 if LS[jj] == "" else np.int64(LS[jj]);
           elif tctype in ['u64','U64'] :
             (areader[prj])[ipline] = 0 if LS[jj] == "" else np.uint64(LS[jj]);
           elif tctype in ['D102','d102','D142','d142','D185','d185'] :
             (areader[prj])[ipline] = Decimal(LS[jj])
           elif tctype in ['Tns','TNS','tns'] :
             # Factor of 1000 because the times are unrecorded for ns
             (areader[prj])[ipline] = dt_int + np.int64(LS[jj])*1000;
           elif tctype in ['Tus','TUS','tus'] :
             # Factor of 1000 because the times are unrecorded for ns
             (areader[prj])[ipline] = dt_int + np.int64(LS[jj]);
           elif tctype in ['float64','F64','f64'] :
             (areader[prj])[ipline] = dt_int + np.float64(LS[jj]);
           elif tctype in ['string','Str','STR','str'] :
             (areader[prj])[ipline] = LS[jj]; 
           elif tctype in ['c2','C2'] :
             (areader[prj])[ipline] = LS[jj][0:2]; 
           elif tctype in ['c1','C1'] :
             (areader[prj])[ipline] = LS[jj][0:1]; 
           else :
             print(" -- Unknown jj = " + str(jj) + " for iline = " + str(iline) + " type is " + tctype);
      ipline = ipline + 1;
    iline = iline + 1;
    if ipline >= readN :
      break;

data_converted = table_cols_from_vecs(areader, in_schema) 
table = pa.Table.from_arrays(data_converted, names = aschema.names) 
# 1. Create some sample data

# 2. Define the output file path
file_path = "c0_save_" + targetMtype + ".arrow"

# 3. Create IpcWriteOptions with desired compression
# Available compression codecs are "lz4" and "zstd"
write_options = ipc.IpcWriteOptions(compression="zstd") 

import os; os.makedirs(save_dir,exist_ok=True);
# 4. Open a file sink and create a file writer
with pa.OSFile(save_dir + "\\" + file_path, 'wb') as sink:
    writer = ipc.new_file(sink, table.schema, options=write_options)
    
    # 5. Write the table to the file
    writer.write_table(table)
    
    # 6. Close the writer
    writer.close()

print(f"Arrow IPC file '{file_path}' created with ZSTD compression.")

# Optional: Verify by reading the file
with pa.memory_map(save_dir + "/" + file_path, 'rb') as source:
    reader = ipc.open_file(source)
    read_table = reader.read_all()
    print("\nData read from compressed file:")
    print(read_table)


import os; import pyarrow as pa;  import pyarrow.ipc as ipc;
save_dir = 'c:/users/alanj/Dropbox/py/ptc/save_data'
file_target_work = 'outputDM.arrow_zstd19'
file_target_work = 'rbc_dm.arrow_zstd19';
source = pa.memory_map(save_dir + "/" + file_target_work, 'rb');
reader=ipc.open_file(source); 
read_table = reader.read_all()
read_table = read_table.to_pandas();
##ipc.close_file(source)
source.close(); #reader.close();
print("\nData read from compressed file:")
print(read_table)





import os; import pyarrow as pa;  import pyarrow.ipc as ipc;
save_dir = 'c:/users/alanj/Dropbox/py/ptc/thread_data'
file_target_work = 'rbc_dc.arrow_zstd19'
source = pa.memory_map(save_dir + "/" + file_target_work, 'rb');
reader=ipc.open_file(source); 
read_table = reader.read_all()
read_table = read_table.to_pandas();
##ipc.close_file(source)
source.close(); #reader.close();
print("\nData read from compressed file:")
print(read_table)




import os; import pyarrow as pa;  import pyarrow.ipc as ipc;
save_dir = 'c:/users/alanj/Dropbox/py/ptc/thread_data/d1000000'
file_target_work = 'rbc_dc.arrow_zstd19'
source = pa.memory_map(save_dir + "/" + file_target_work, 'rb');
reader=ipc.open_file(source); 
read_table = reader.read_all()
read_table = read_table.to_pandas();
##ipc.close_file(source)
source.close(); #reader.close();
print("\nData read from compressed file:")
print(read_table)










