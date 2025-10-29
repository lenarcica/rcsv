## Looking at stuff
import pandas as pd; import numpy as np; import os;
config_file = pd.read_csv("c:/users/alanj/Dropbox/py/ptc/test_src/c0.psv", sep="|", header=None, on_bad_lines='warn', na_values=['']);
config_file.columns = ['dc','mtype','col','coltype','placerow'];
dirwrite = 'c:/users/alanj/Dropbox/py/ptc/test_data/'
tot_n = 100000000; kt = 'D';  use_config = config_file[config_file['dc'] == kt].reset_index(inplace=False, drop=True);

fakempids = ['B','N','F','S','m','X'];
fakesrcs = ['B','N','I','X'];
fakesyms = ['aapl','btc','ibm','intel','stuff'];
fakestrs = ['','gd','ftx','','kdi','twer'];
Allmtype = list(set(use_config['mtype'].to_list()))
def MakeChunks(myFT: pd.DataFrame, write_n:int = 100000) :
  MyD = {};
  for jj in range(len(myFT)) :
    if (myFT.iloc[jj].col == 'mtype') :
      MyD[jj] = np.random.choice(np.unique(myFT['mtype']), size=write_n);
    elif (myFT.iloc[jj].col in ['mtime','time','rtime']) :
      MyD[jj] = rand_times = np.random.randint(low=100,high=440320,size=write_n, dtype='l');
    elif (myFT.iloc[jj].col in ['dlt','dlts','delta']) :
      rand_dlts = np.random.randint(low=-50,high=230,size=write_n,dtype='l');
      rand_dlts[(rand_dlts<=3) & (rand_dlts >= -3) ] = 0;
      MyD[jj] = rand_dlts;
    elif (myFT.iloc[jj].col in ['oid','oids','OID']) :
      MyD[jj] = np.random.randint(low=1000000, high=9999999, size=write_n, dtype='l')
    elif (myFT.iloc[jj].col in ['oldoid','oldoids','oldis','oldid']) :
      MyD[jj] = np.random.randint(low=1000000, high=9999999, size=write_n, dtype='l')
    elif (myFT.iloc[jj].col in ['mpid','mpids','bmpid','smpid']) :
      MyD[jj] = np.random.choice(fakempids,size=write_n);
    elif (myFT.iloc[jj].col in ['qty','qtys','QTY','bqty','sqty']) :
      rand_qty = np.random.uniform(0,600, write_n);
      MyD[jj] =  [str(x) for x in np.floor(rand_qty*100) / 100];
    elif (myFT.iloc[jj].col in ['price','Price','bprice','sprice','PRICE']) :
      rand_prices = np.floor(np.random.uniform(-20,20000, write_n)).astype(np.int64);
      MyD[jj] = [("" if x <= 0 else (str(x)[0:(len(str(x))-2)] + '.' + str(x)[(len(str(x))-2):len(str(x))])) for x in rand_prices];
    elif (myFT.iloc[jj].col in ['sym','symbol','SYM','Sym','ticker']) :
      MyD[jj] = np.random.choice(fakesyms,size=write_n); 
    else  :
      MyD[jj] = np.random.choice(fakestrs,size=write_n); 
  df = pd.DataFrame(MyD);
  return(df.astype(str).apply(lambda x: ','.join(x), axis=1))

myFT =use_config[use_config['mtype'] == "T"].reset_index(inplace=False, drop=True);
FT = MakeChunks(myFT,100)

def WriteFullChunk(write_n:int = 100000) :
  nF = np.random.randint(0,200, size = len(Allmtype));
  tots = np.round(nF * write_n / np.sum(nF)).astype(np.int64);
  tots[len(tots)-1] = write_n - np.sum(tots[0:(len(tots)-1)])
  myx = None;
  for k in range(len(Allmtype)) :
    if tots[k] > 0 :
      MyN = MakeChunks(use_config[use_config['mtype'] == Allmtype[k]].reset_index(inplace=False, drop=True),tots[k]);
      if myx is None :
        myx = MyN;
      else :
        myx = pd.concat([myx,MyN])
  return(myx.sample(frac=1).reset_index(inplace=False, drop=True))

chunk_size = 1000000
Chunk1 = WriteFullChunk(100000)
Allmtype = list(set(use_config['mtype'].to_list()))
myFT = use_config[use_config['mtype'] == Allmtype[0]].reset_index(inplace=False, drop=True);
AChunk = MakeChunks(myFT, 10000);
tot_chunks = int(np.ceil(tot_n /chunk_size));
filetarget = dirwrite+ '/d0_' + str(tot_n) + '.csv';
os.makedirs(dirwrite,exist_ok=True);
for tii in range(tot_chunks) :
  AChunk = pd.DataFrame({'0':WriteFullChunk(chunk_size)});
  AChunk['1'] = tii*chunk_size + np.arange(chunk_size);
  AChunk = AChunk.astype(str).apply(lambda x: ','.join(x), axis=1);
  print("Save Chunk tii = " + str(tii) + "/" + str(tot_chunks) + " total done is " + str(chunk_size * tii) + "/" + str(tot_n))
  with open(filetarget,'at') as f :
     f.write("\n".join(AChunk.to_list()));
