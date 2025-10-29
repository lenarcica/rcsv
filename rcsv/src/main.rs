//mod kdefines;

use std::{error::Error, fs::File, path::Path};
use lib_rcsv::read_config_psv; 
extern crate proc_macro;
use std::env;
use proc_macro::TokenStream;
//use quote::quote;
use syn::{parse_macro_input, LitStr};
use std::io::Read;
use std::path::PathBuf;
use syn;  use quote;
use std::time;

//use typed_arrow::{prelude::*, schema::SchemaMeta};
//use typed_arrow::{Dictionary, TimestampTz, Millisecond, Utc, List};

// Try to run generation of implementations here?
read_config_psv!("c0.psv");

struct BufFillExample {
  pub tlen: usize,
  pub oni: usize,
  pub v_00: u8,
  pub v_01_time: u64,
  pub v_02_dlt: u64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("---- Initiate Algorithmic test");
    // Print all arguments
    println!("Your Supplied Arguments are: {:?}", args);
    println!("  Argument 1 is {}", args.iter().nth(1).unwrap_or(&"not given".to_string()).to_string());
    println!("  Arument 1 again is {}.", args.iter().nth(1).unwrap_or(&"Dohzy!".to_string()).to_string());
    let verbose:i64 =   match args.iter().nth(1).unwrap_or(&"0".to_string()).to_string().parse() { Ok(val) => val, Err(e) => {println!("Hey supply verbose correctly. Error is {}",e); 0}};
    let file_name:String = args.iter().nth(2).unwrap_or(&"c:/users/alanj/Dropbox/py/ptc/test_data/d0_1000000.csv".to_string()).to_string();
    let save_dir:String = args.iter().nth(3).unwrap_or(&"c:/users/alanj/Dropbox/py/ptc/thread_data".to_string()).to_string();
    let file_pt = file_name.split(|c| (c == '/') || (c=='\\')).last().unwrap_or("unnamed").replace(".csv","");
    let save_dir = format!("{}/{}",save_dir,file_pt);
    let my_path = std::path::Path::new(&save_dir);
    if !my_path.is_dir() {
      std::fs::create_dir_all(save_dir.clone()).expect(&format!("Unable to create directory {}.", save_dir));
    }
    println!(" Note file_pt = {} and we will save to \"{}\".", file_pt, save_dir);

    let buff_size:u64 =   match args.iter().nth(4).unwrap_or(&"40000".to_string()).parse::<u64>() { Ok(val)=>val, Err(e) => {println!("Could not read thread choice."); 40000 as u64}};
    let n_threads:i8 = match args.iter().nth(5).unwrap_or(&"8".to_string()).parse()
                         { Ok(val)=>val, Err(e)=>{println!("Failed to parse n_threads"); 8}}; 
      // Current computer has 8 threads, approx typical server size
    // Access specific arguments by index (remember args[0] is the program name)
    main_algo(verbose, file_name, save_dir, file_pt, buff_size, n_threads);
}
//cargo run --release -- 2 c:/users/alanj/pjd/ptc/testdata/d0_100000000.csv c:/users/alanj/pjd/ptc/thread_data 200000 8
/*
     0:mtype   |c1  | 0| 0
    1:rtime   |tns | 1| 1
    2:dlt     |i64 | 2| 2
    3:src     |c1  | 3| 3
    4:sym     |str | 4| 4
    5:bmpid   |c1  | 5| 5
    6:bprice  |d102| 6| 6
    7:bqty    |i64 | 7| 7
    8:brank   |N   |-1| 8
    9:btxt    |N   |-1| 9
   10:smpid   |N   |-1|10
   11:sprice  |d102| 8|11
   12:sqty    |i64 | 9|12
   13:srank   |str |10|13
   14:stxt    |N   |-1|14
   15:flags   |str |11|15

   RbTable(D,M,9):
----------------
    0:mtype   |c1  | 0| 0
    1:rtime   |tns | 1| 1
    2:dlt     |i64 | 2| 2
    3:sym     |str | 3| 3
    4:oid     |i64 | 4| 4
    5:src     |str | 5| 5
    6:price   |d102| 6| 6
    7:qty     |d102| 7| 7
    8:flags   |str | 8| 8

 */
fn main_algo(verbose:i64,file_name:String, save_dir:String, file_pt:String, buff_size:u64, n_threads: i8)  {
  macro_rules!pv{
    {$vb:expr,$($rest:tt)*} => {
      if (verbose > $vb) { println![$($rest)*] }
    }
  }
  pv![0,"Welcome to main Algorithm"];
  //read_config_psv!(("c0.psv").parse().unwrap());
  let mut rbc2: ReadBufferC2 = ReadBufferC2::new(100 as usize,0,0, false);
  println!("Parsing a single line");
  let line_a = "T,1023,203,ASym,m,Q,123.45,67,sds,gds,Q,678.32,100,xs,,";
  rbc2.parse_one_line(&line_a.to_string(),0);
  println!("Trying to display the line we read. ->{}.", line_a);
  rbc2.display_line(0);
 
  let n_days:i64 = 20301; let buff_capacity:u64 = 8*1048576;
  let add_time:i64 =  (24 * 60 * 60 as i64) * n_days * (1000000000 as i64);
  let read_capacity = 1024;
  /** // Historical Debug Unit Tests
  pv![1,"----------------------------------------------------------------"]; 
  pv![1," --- main.rs -- Test1:  lets generate a complete reader and attack again"];
  let mut cr:CompleteReader = CompleteReader::new(100 as usize, 0,0,"c:/users/alanj/Dropbox/py/ptc/save_data".to_string(), add_time, true, buff_capacity,verbose);
  cr.rbc2.parse_one_line(&"T,1023,203,ASym,m,Q,123.45,67,sds,gds,Q,678.32,100,xs,,".to_string(),0);
  let line_b = "M,3023,,TwoSim,2922,N,130.32,98,qsv";
  let line_c = "M,3027,,TwoSim,3222,B,230.32,38,qsv";
  let line_d = "M,3029,,OthSim,322,Y,330.32,38,qsv";
  let line_e = "T,3032,10,OthSim,362,Y,330.32,38,qsv";
  let line_f = "M,3033,20,Sim3,352,Y,521432,38,tfw";
  cr.rbdm.parse_one_line(&line_b.to_string(),0);
  cr.rbdm.parse_one_line(&line_c.to_string(),1);
  cr.rbdm.parse_one_line(&line_d.to_string(),2);
  cr.rbdm.parse_one_line(&line_b.to_string(),3);
  cr.rbdm.consider_parse_one_line(&line_e.to_string(),4);
  cr.rbdm.consider_parse_one_line(&line_f.to_string(),5);
  println!(" here is read line_b->{}",line_b);
  cr.rbdm.display_line(0);
  cr.rbdm.display_line(1);
  cr.rbdm.display_line(2);
  println!(" Here we will try and write to a file");
  cr.rbdm.flush_current_data();
  println!("  Well if successful we saved that file.");
  **/

  //let orig_file_dir = "c:/users/alanj/Dropbox/py/ptc/test_data/";
  //let save_file_dir = "c:/users/alanj/Dropbox/py/ptc/thread_data/d1000000";
  //let config_file_name = "c0_0.csv"; // It is too late at this point to try other configuration
  //let file_name = "d0_1000000.csv";
  //let full_file_name = format!("{}/{}", orig_file_dir, file_name);
  pv![1,"Estimating file size of {}", file_name];
  let file_size = {
    let temp_file = Arc::new(File::open(file_name.clone()).expect(&format!("Failed to open file {}",file_name.clone())));
    temp_file.metadata().expect("file_size should have returned a positive length").len() };
  pv![1," --- main_algo.rs:: Test 2: Initial Arc read of file said file_size = {}", file_size];
  let chunk_size = file_size / n_threads as u64;
  let mut handles = vec![];

  let measure_cr0 = std::time::Instant::now();
  let mut cr_0 = CompleteReader::new(buff_size as usize, 0, -1, save_dir.clone(),add_time, true, buff_capacity,verbose);
  for i_thread in 0..n_threads {
    let i_thread:i8 = i_thread.try_into().unwrap();
    //let file_clone = Arc::clone(&file);
    let full_file_name_clone = file_name.clone();
    let start_offset = i_thread as u64 * chunk_size;
    let end_offset = if i_thread == n_threads - 1 {   file_size } else {
       ((i_thread + 1) as u64) * chunk_size };
    let save_file_dir_clone = save_dir.clone();
    let mut cr_cl = cr_0.clone_update(start_offset,i_thread);
    let handle = std::thread::spawn(move || {
      // WARNING: Do not use Arc over multiple handles as BufReader will suck.  Need 1 handle per
      // bufreader!
      let file_clone = File::open(&full_file_name_clone).expect(&format!("Thread {} failed to open file {}", i_thread, &full_file_name_clone));
      let max_bytes:u64 =  match u64::try_from(end_offset-start_offset) { Ok(val)=>val,Err(_e)=>0 };
      //println!("Hello thread {} we go from {} to {} - with cr_cl.start_offset={}", i_thread, start_offset,end_offset, cr_cl.start_offset);
      let v_total_read_lines = cr_cl.process_all_lines_d(file_clone, max_bytes);
      pv![3," -- Thread {} says I'm done: loaded total={}: [{}].", i_thread, 
        v_total_read_lines.iter().sum::<u32>(), 
        v_total_read_lines.iter().map(|a| format!("{}",a)).collect::<Vec<String>>().join(",")];
      //cr_cl.finish();
      //let mut reader = BufReader::new(file_clone);
      //reader.seek(std::io::SeekFrom::Start(if start_offset == 0 { 0 } else { start_offset-1} )).expect("Failed to seek");
      //let mut lines_iter = reader.lines();
      //if let Some(first_line) = lines_iter.next() {
      //  let line_content = first_line.expect("trying to unlock first line");
      //  if line_content.starts_with("\n") {
      //    println!("Exactly broke on a line at {}", if start_offset==0 {0} else {start_offset-1} );
      //  }
      //  // we actually almost always trash this line
      // }
     });
     handles.push(handle);
  }
  for handle in handles {
        handle.join().expect("Thread panicked");
  }
  //println!("We reached end of all threads!");
  cr_0.finish();
  pv![1," --- main.rs  Completion, cr0.finish has been called."];
  pv![1,"Time elapsed: {:?}", measure_cr0.elapsed()];
  pv![1," --- Version without loading done."];
  let mut handles_1 = vec![];
  let measure_cr1 = std::time::Instant::now();
  let mut cr_1 = CompleteReader::new(buff_size as usize, 0, -1, save_dir.clone(),add_time, true, buff_capacity,verbose);
  for i_thread in 0..n_threads {
    let i_thread:i8 = i_thread.try_into().unwrap();
    //let file_clone = Arc::clone(&file);
    let full_file_name_clone = file_name.clone();
    let start_offset = i_thread as u64 * chunk_size;
    let end_offset = if i_thread == n_threads - 1 {   file_size } else {
       ((i_thread + 1) as u64) * chunk_size };
    let save_file_dir_clone = save_dir.clone();
    let mut cr_cl = cr_1.clone_update(start_offset,i_thread);
    let handle = std::thread::spawn(move || {
      // WARNING: Do not use Arc over multiple handles as BufReader will suck.  Need 1 handle per
      // bufreader!
      let file_clone = File::open(&full_file_name_clone).expect(&format!("Thread {} failed to open file {}", i_thread, &full_file_name_clone));
      let max_bytes:u64 =  match u64::try_from(end_offset-start_offset) { Ok(val)=>val,Err(_e)=>0 };
      let mut reader = std::io::BufReader::with_capacity(buff_capacity.try_into().unwrap(), file_clone);
      reader.seek(std::io::SeekFrom::Start(if start_offset == 0 { 0 } else { start_offset-1} )).expect("Failed to seek");
      let mut buffer = Vec::new();
      reader.read_until(b'\n', &mut buffer).expect("No newline ever found!");
      let len_first_line:u64 = buffer.len() as u64;
      let total_bytes_to_read:u64 = end_offset  + (if start_offset == 0 { 0 } else { 1 }) - (start_offset + len_first_line);
      let v_total_read_lines = cr_cl.process_all_lines_by_type_d(&mut reader, total_bytes_to_read,i_thread);
      pv![1," -- All Lines By Type Thread {} says I'm done: loaded total={}: [{}].", i_thread, 
        v_total_read_lines.iter().sum::<u32>(), 
        v_total_read_lines.iter().map(|a| format!("{}",a)).collect::<Vec<String>>().join(",")];
     });
     handles_1.push(handle);
  }
  for handle in handles_1 {
    handle.join().expect("Thread on 1 panicked");
  }
  pv![1,"Time elapsed: {:?}", measure_cr1.elapsed()];
  cr_1.finish();
  //read_config_psv!(("c0.psv").parse().unwrap());
}


