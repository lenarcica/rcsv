// Procedural Macro defined for CSV reading
// Alan Lenarcic 2025
// lib.rs in lib_rcsv
extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;

use std::str::FromStr;
use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2};
use quote::quote;
use csv;
use syn::{parse_macro_input, LitStr, parse_str};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::io::{self, Write};
use std::sync::Arc;
use std::vec::Vec;
use std::fmt;

//use arrow_schema;
//use arrow_schema::Schema;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::array::{Float64Array, Int64Array,Decimal64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use arrow::datatypes::DataType::Timestamp;
use arrow::array::ArrayRef;
//use arrow::array::TimestampNanosecondArray;

use std::collections::HashSet;


const NAF64:f64 = -99.0;
const NAI64:i64 = -99;
const NAD102:i64 =  -9900;
const NAD185:i64 = -9900000;
const NAD142:i64 = -9900;
const NATNS:i64 = -99 * 1000000000;
const NATUS:i64 = -99 * 1000000;

const ORIG_TYPE_TEXT: &str = "u32";
const ARROW_IPC_OR_PARQUET: u8 = 1;
const ZSTD_LEVEL:u8= 13;

use arrow_ipc;
/*
struct BorrowWriter<'a> {
  borrowed_opend_file:&'a mut std:fs:File,
  pub write_loc : std ::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<&'a mutFile>>>
}
impl BorrowWriter<'_> {
  pub fn new(&lent_opened_file) {


  }
}
*/
/**
struct WriterLoc {
    pub dc : String, pub mtype : String, pub save_dir : String, pub schema :
    arrow_schema::Schema, pub want_fn : String, 
    pub write_options: arrow_ipc::writer::IpcWriteOptions, 
    //pub opened_file : std::fs::File,
    //borrowed_opened_file:&'a mut std::fs::File,
    //pub some_file : std::fmt::Result <std::fs::File, std::fmt:: Error > , 
    //pub bw : Some(BorrowedWriter);
    //pub write_loc : Option<std::sync::Weak<std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<&'a mut File>>>>>
    pub write_loc : std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>>
} 
impl <'a> Clone for WriterLoc {
    fn clone(& self) -> Self
    {
        return WriterLoc
        {
            dc : self.dc.clone(), mtype : self.mtype.clone(), save_dir :
            self.save_dir.clone(), schema : self.schema.clone(), want_fn :
            self.want_fn.clone(), write_options : self.write_options.clone(),
            //opened_file: self.opened_file.try_clone().expect("open clone"),
            write_loc: self.write_loc.clone()
        }
    }
} 
impl WriterLoc {
    pub fn new(in_dc : String, in_mtype : String, in_save_dir : String, 
        in_schema : arrow :: datatypes :: Schema) -> Self {
      println!("(writer_loc{},{} - in_save_dir={}) -- we initiate, hope it works.",in_dc.clone(),in_mtype.clone(),in_save_dir);
      let want_fn =
        format!("{}/rbc_{}{}.arrow_zstd19", in_save_dir.clone(),
        in_dc.clone(),in_mtype.clone()); 
      let mut opened_file = File ::create(want_fn.clone()).expect(&format!("Expect to open {}", want_fn.clone())); 
      let write_options = arrow_ipc::writer::IpcWriteOptions::try_new(
           8,false,arrow_ipc::MetadataVersion::V5).expect("write_opentions IpcWriteOption fail on try_new"
           ).try_with_compression(Some(arrow_ipc::CompressionType::ZSTD)).expect("write_options fail on try_with_compression");
      let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options( opened_file, &in_schema.clone(),
        write_options.clone()).expect(&format!("Opened the file writer or maybe failed {}{}",in_dc.clone(),in_mtype.clone()));
        return WriterLoc
        {
            dc : in_dc.clone(), mtype : in_mtype.clone(), save_dir :
            in_save_dir.clone(), schema : in_schema.clone(), want_fn :
            want_fn, write_options : write_options, //opened_file:opened_file,
            write_loc :  std::sync::Arc::new(std::sync::Mutex::new(writer))
        }
    }
    pub fn get_thread_safe_writer(&self) -> std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>> {
        Arc::clone(&self.write_loc)
    }
    //pub fn extract_writer
    //pub fn lock_data(&self) -> Option<std::sync::MutexGuard<'_, arrow_ipc::writer::FileWriter<&'_ mut File>>> {
    //    match &self.write_loc {
    //      Some(x) => {match x.upgrade() { Some(y)=> return Some(y.lock().unwrap()), None=> return None }},
    //      None => return None
    //    }
    //}
    pub fn finish(& mut self) {
      let mut writer = self.write_loc.lock().unwrap();
      writer.finish().expect(" Should finish writing. ");
    }
} 
impl Drop for WriterLoc {
  fn drop(& mut self) { (*self).finish(); } 
}
**/
fn get_use_requirements(input_coltypes: Vec<String>) ->Vec<TokenStream2> {
  let mut use_requirements:Vec<TokenStream2>  = Vec::<TokenStream2>::new();
  use_requirements.push(TokenStream2::from_str("use arrow;").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use std::io::{BufRead,Seek};").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use arrow_schema;").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use arrow::datatypes::{DataType,Field};").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use arrow::record_batch::RecordBatch;").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use arrow::array::ArrayRef;").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use std::sync::Arc;").expect("This should aways parse"));
  if ARROW_IPC_OR_PARQUET == 0 {
    use_requirements.push(TokenStream2::from_str("use arrow_ipc::writer::{CompressionContext,FileWriter, IpcWriteOptions};").expect("This should always parse"));
  } else {
    use_requirements.push(TokenStream2::from_str("use parquet::arrow::arrow_writer::ArrowWriter;").expect("AW"));
  }
  use_requirements.push(TokenStream2::from_str("use parquet::file::properties::{WriterProperties, WriterVersion};use parquet::basic::Compression;").expect("CompressionWriter"));
  use_requirements.push(TokenStream2::from_str("use zstd::zstd_safe::{CParameter,WriteBuf};").expect("This should aways parse"));
  use_requirements.push(TokenStream2::from_str("use std::fs::OpenOptions;").expect("Open Options should be there"));
  if input_coltypes.iter().any(|x| (x == "str") || (x== "STR") || (x=="Str")) {
    use_requirements.push(TokenStream2::from_str("use arrow::array::{StringArray};").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "C1") || (x== "C2") || (x=="c1") || (x=="c2")) {
    use_requirements.push(TokenStream2::from_str("use arrow::array::FixedSizeBinaryArray;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "i64") || (x== "I64") || (x=="DLT64") || (x=="dlt64") || (x=="dlt") || (x=="DLT")) {
    use_requirements.push(TokenStream2::from_str("use arrow::array::Int64Array;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "d102") || (x== "D102") || (x=="D185") || (x=="d185") || (x=="D142") || (x=="D142")) {
    use_requirements.push(TokenStream2::from_str("use arrow::array::Decimal64Array;").expect("This should aways parse"));
    use_requirements.push(TokenStream2::from_str("use arrow::compute::cast;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "u64") || (x== "U64")  ) {
    use_requirements.push(TokenStream2::from_str("use arrow::array::UInt64Array;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "f64") || (x== "F64") ) { 
    use_requirements.push(TokenStream2::from_str("use arrow::array::Float64Array;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "TNS") || (x == "tns") || (x=="Tns") ) { 
    use_requirements.push(TokenStream2::from_str("use arrow::array::TimestampNanosecondArray;").expect("This should aways parse"));
  }
  if input_coltypes.iter().any(|x| (x == "TUS") || (x == "tus") || (x=="Tus") ) { 
    use_requirements.push(TokenStream2::from_str("use arrow::array::TimestampMicrosecondArray;").expect("This should aways parse"));
  }
  return use_requirements
}

macro_rules!map_rust_type{
  ($xtype:expr) => {
     match ($xtype.as_str()) {
       "i64"|"I64" => "i64",
       "dlt64"|"dlt"|"DLT"|"DLT64" => "i64",
       "u64"|"U64" => "u64",
       "f64"|"Float64"|"F64" => "f64",
       "d102"|"D102" => "i64",
       "D142"|"d142" => "i64",
       "D185"|"d185" => "i64",
       "TNS"|"Tns"|"tns" => "i64",
       "i32"|"I32" => "i32",
       "u32"|"U32" => "u32",
       "TUS"|"Tus"|"tus" =>"i64",
       "str"|"Str"|"STR" =>"String",
       "c2"|"C2"=>"u16",
       "c1"|"C1"=>"u8",
       _ => "Oops" 
     }
  }
}
macro_rules!map_rust_default{
  ($xtype:expr) => {
     match ($xtype.as_str()) {
       "i64"|"I64" => format!("{} as i64",NAI64),
       "dlt64"|"dlt"|"DLT"|"DLT64" => format!("{} as i64",0),
       "u64"|"U64" => format!("{} as u64",0),
       "u32"|"U32" => format!("{} as u32",0),
       "f64"|"Float64"|"F64" => format!("{} as f64",NAF64),
       "d102"|"D102" => format!("{} as i64",NAI64),
       "D142"|"d142" => format!("{} as i64",NAI64),
       "D185"|"d185" => format!("{} as i64",NAI64),
       "TNS"|"Tns"|"tns" => format!("{} as i64",NATNS),
       "i32"|"I32" => format!("{} as i32",NAI64),
       "TUS"|"Tus"|"tus" => format!("{} as i64", NATUS),
       "str"|"Str"|"STR" =>"\"\".to_string()".to_string(),
       "c1"|"C1"=>"b' ' as u8".to_string(),
       "c2"|"C2"=>"[b' ',b' '] as u16".to_string(),
       _ => "Oops".to_string() 
     }
  }
}

macro_rules!map_arrow_type{
  ($xtype:expr) => {
     match ($xtype.as_str()) {
       "i64"|"I64" => "arrow::datatypes::DataType::Int64",
       "u64"|"U64" => "arrow::datatypes::DataType::UInt64",
       "u32"|"U32" => "arrow::datatypes::DataType::UInt32",
       "dlt64"|"dlt"|"DLT64"|"DLT" => "arrow::datatypes::DataType::Int64",
       "f64"|"Float64"|"F64" => "arrow::datatypes::DataType::Float64",
       "d102"|"D102" => "arrow::datatypes::DataType::Decimal64(10 as u8,2 as i8)",
       "D142"|"d142" => "arrow::datatypes::DataType::Decimal64(14 as u8,2 as i8)",
       "D185"|"d185" => "arrow::datatypes::DataType::Decimal64(18 as u8,5 as i8)",
       "TNS"|"Tns"|"tns" => "arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond,None)",
       "TUS"|"us"|"tus" => "arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Mircosecond,None)",
       "i32"|"I32" => "arrow::datatypes::DataType::Int32",
       "str"|"Str"|"STR" =>"arrow::datatypes::DataType::Utf8",
       "c2"|"C2"=>"arrow::datatypes::DataType::FixedSizeBinary(2)",
       "c1"|"C1"=>"arrow::datatypes::DataType::FixedSizeBinary(1)",
       _ => "BADDATA" 
     }
  }
}

macro_rules!map_arrow_array_type{
  ($xtype:expr) => {
     match ($xtype.as_str()) {
       "i64"|"I64" => "Int64Array",
       "u64"|"U64" => "UInt64Array",
       "dlt64"|"dlt"|"DLT64"|"DLT" => "arrow::array::Int64Array",
       "f64"|"Float64"|"F64" => "arrow::array::Float64Array",
       "d102"|"D102" => "arrow::array::Decimal64Array",
       "D142"|"d142" => "arrow::array::Decimal64Array",
       "D185"|"d185" => "arrow:array::Decimal64Array",
       "TNS"|"Tns"|"tns" => "arrow::array::TimestampNanosecondArray",
       "i32"|"I32" => "arrow::array::Int32Array",
       "u32"|"U32" => "arrow::array::UInt32Array",
       "TUS"|"Tus"|"tus" =>"arrow::array::TimestampMicrosecondArray",
       "str"|"Str"|"STR" =>"arrow::array::StringArray",
       "c2"|"C2"=>"arrow::array::FixedSizeBinaryArray(2)",
       "c1"|"C1"=>"arrow::array::FixedSizeBinaryArray(1)",
       _ => "BADDATA" 
     }
  }
}
struct RTbSchema {
  pub tlen: usize,
  pub dc: char, pub mtype: char,
  pub v_01_col: Vec<String>,
  pub v_02_coltype: Vec<String>,
  pub v_03_vloc: Vec<i8>,
  pub v_04_origloc: Vec<i8>
}
fn gen_field_array_type(ix:i8,jx:i8,nm: &str, typ:&str) -> TokenStream2 {
 //Field::new("time",Timestamp(TimeUnit::Nanosecond,None),false));
  let MkStr:String = match typ {
    "TNS" | "Tns" | "tns"  => {
       //vs.push(Field::new("time",Timestamp(TimeUnit::Nanosecond,None),false));
       //va.push(Arc::new(TimestampNanosecondArray::from((*self).v_time[0..(*self).iprint].to_vec())));
       // let timestamp_array: PrimitiveArray<TimestampNanosecondType> =/ TimestampNanosecondArray::from_vec(i64_data, None).into()
       //
       format!("Arc::new(arrow::array::TimestampNanosecondArray::from(self.v_{:02}_{:02}_{}[0..self.i_write].to_vec())) as ArrayRef", ix,jx,nm)
       //format!("Arc::new(TimestampNanosecondArray::from(self.v_{:02}_{:02}_{}[0..self.i_write])) as ArrayRef", ix,jx,nm) 
    },
    "TUS" | "Tus" |  "tus"  => {
       format!("Arc::new(arrow::array::TimestampMicrosecondArray::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec()) ) as ArrayRef",
          ix,jx,nm) },
    // "d142" |"D142"   => {
    //    format!("Arc::new(Int64Array::from(vec![0 as i64; self.i_write])) as ArrayRef")
    //},
    "D185" | "d185" => {
       format!("Arc::new(arrow::array::Decimal64Array::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec()).with_precision_and_scale( 18, 5).unwrap()) as ArrayRef", ix, jx,nm)},
       //format!("{{ let decimal_array_uncast = Decimal64Array::from(self.v_{:02}_{:02}_{}[0..self.i_write]); \
       //            let tgt_type = DataType::Decimal64(18,5); let d_a = cast(&decimal_array_uncast,&tgt_type).expect(\"18 5 cast did not work. {},{},{}.\"); \
       //            let final_decimal_array = d_a.as_any().downcast_ref::<Decimal64Array>().unwrap(); \
       //            Arc::new(final_decimal_array) as ArrayRef }}", ix,jx,nm,ix,jx,nm) },
    "D142" | "d142" => {
       format!("Arc::new(arrow::array::Decimal64Array::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec()).with_precision_and_scale( 14, 2).unwrap()) as ArrayRef", ix, jx,nm)},
    //    format!("{{ let decimal_array_uncast = Decimal64Array::from(self.v_{:02}_{:02}_{}[0..self.i_write]); \
    //               let tgt_type = DataType::Decimal64(14,2); let d_a = cast(&decimal_array_uncast,&tgt_type).expect(\"10 2 cast did not work {},{},{}\"); \
    //               let final_decimal_array = d_a.as_any().downcast_ref::<Decimal64Array>().unwrap(); \
    //               Arc::new(final_decimal_array) as ArrayRef }}", ix,jx,nm,ix,jx,nm) },
    "D102" | "d102" => {
         format!("Arc::new(arrow::array::Decimal64Array::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec()).with_precision_and_scale( 10, 2).unwrap()) as ArrayRef", ix, jx,nm)},
        //format!("{{ let decimal_array_uncast = Decimal64Array::from(self.v_{:02}_{:02}_{}[0..self.i_write].to_vec()); \
        //           let tgt_type = DataType::Decimal64(10,2); let d_a = arrow::compute::cast(&decimal_array_uncast,&tgt_type).expect(\"10 2 cast did not work {},{},{}\"); \
        //           let cast_options = arrow::compute::cast::CastOptions::default();
        //           let final_decimal_array = d_a.as_any().downcast_ref::<Decimal64Array>().unwrap(); \
        //           Arc::new(final_decimal_array) as ArrayRef }}", ix,jx,nm,ix,jx,nm) },
    "c2" | "C2"  => {
       format!("{{ let vv8: Vec<Vec<u8>> = self.v_{:02}_{:02}_{}.iter().take(self.i_write).map(|&b| b as [u8;2]).collect(); \
                   Arc::new(arrow::array::FixedSizeBinaryArray::try_from_iter(vv8.iter()).expect(\"Fixed Size 2 Cast\"))as ArrayRef }} ", ix,jx,nm)},
       //format!("{{ Arc::new(arrow::array::BinaryArray::try_from_iter( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec())) as ArrayRef }} ", ix,jx,nm)},
    "C1" | "c1"  => {
       //format!("{{ Arc::new(arrow::array::BinaryArray::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec())) as ArrayRef }} ", ix,jx,nm)},
       format!("{{ let vv8: Vec<Vec<u8>> = self.v_{:02}_{:02}_{}.iter().take(self.i_write).map(|&b| vec![b]).collect(); \
                  Arc::new(arrow::array::FixedSizeBinaryArray::try_from_iter(vv8.iter()).expect(\"This v8 iter iter should evaulate\")) as ArrayRef }} ",ix,jx,nm)},

    "STR" | "str"  | "Str" => {
       //format!("{{ Arc::new(arrow::array::BinaryArray::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec())) as ArrayRef }} ", ix,jx,nm)},
       format!("{{    let mut my_str:Vec<&str> = self.v_{:02}_{:02}_{}.split('|').collect(); if my_str.len() > 0 {{ my_str.pop(); }}  \
                \n    Arc::new(arrow::array::StringArray::from( my_str)) as ArrayRef }}",ix,jx,nm) }
    _ => {
       format!("Arc::new({}::from( (self.v_{:02}_{:02}_{}[0..self.i_write]).to_vec())) as ArrayRef",
          map_arrow_array_type!(typ.to_string()),ix,jx,nm) }
  };
  return TokenStream2::from_str(MkStr.as_str()).expect(&format!("gen_field_array_type TokenStream conversion failed on {},{},{},{}", ix,jx,nm,typ));
}
fn gen_csv_part_read(ix:i8, jx:i8, nm: &str, typ:&str) -> TokenStream2 {
  if jx < 0 {
    return TokenStream2::from_str("let no_read = parts.next()").expect(&format!("gen_csv_part_read failed gen_csv_part_read {},{},{},{}",ix,jx,nm,typ));
  }
  let parsing_string = match typ {
    "c1"|"C1" => format!("match u8::try_from(parts.next().and_then(|s| Some(s.trim_start().chars().nth(0).unwrap_or('X')) ).unwrap_or_else( \
       || {{println!(\"rb{{}}{{}}_t{{}}: in_i={{}}, Expected to parse c1 for col[{},{}.{}]: |{{}}|\", self.dc,self.mtype,self.thread_i, in_i,&line); 'X'}})) {{ Ok(val)=>val,Err(_e)=>b' '}} ", ix,jx,nm),
    "c2"|"C2" => format!("parts.next().and_then(|s| {{ let s = s.trim_start().chars(); Some([s.nth(0).unwrap_or('X') as u8,s.nth(1).unwrap_or('X') as u8]) }}).unwrap_or_else( \
       || {{println!(\"Expected to parse c1 for col[{},{}.{}\"); [b'X',b'X']}})", ix,jx,nm),
    "f64"|"F64" => format!("parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{ s.parse::<f64>().ok() }}).unwrap_or_else( \
       || {{ println!(\"Expect to parse i64 for col[{},{},{}]\"); {} }} ) as f64",NAF64,ix,jx,nm, NAF64),
    "u64"|"U64" => format!("parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<u64>().ok() }}).unwrap_or_else( \
       || {{ println!(\"Expect to parse dlt as i64 for col[{},{},{}]\");  {} }} )",0, ix,jx,nm, 0),
    "dlt64"|"dlt"|"DLT64"|"DLT" => format!("parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<i64>().ok() }}).unwrap_or_else( \
       || {{ println!(\"Expect to parse dlt as i64 for col[{},{},{}]\");  {} }} )",0, ix,jx,nm, 0),
    //"i64"|"I64" => format!("parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<i64>().ok() }}).unwrap_or_else( \
    //   || {{ println!(\"rb{{}}{{}}_t{{}}: Expect to parse i64 for in_i={{}}, col[{},{},{}]\", self.dc.to_lowercase(),self.mtype.to_lowercase(),self.thread_i,in_i);  {} }} )",
    //   NAI64, ix,jx,nm, NAI64),
    "i64"|"I64" => format!("match parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<i64>().ok() }}) {{ Some(val) => val, None => \
       {{ println!(\"rb{{}}{{}}_t{{}}: Expect to parse i64 for in_i={{}}, col[{},{},{}] - |{{}}|\", self.dc.to_lowercase(),self.mtype.to_lowercase(),self.thread_i,in_i,&line);  {} }} }}",
       NAI64, ix,jx,nm, NAI64),
    "TNS"|"Tns"|"tns" => format!("(self.add_time as i64) + parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<i64>().ok() }}).unwrap_or_else( \
       || {{ println!(\"Expect to parse i64 from TNS for col[{},{},{}]\");  {} }} )",NAI64,ix,jx,nm,NAI64),
    "TUS"|"Tus"|"tus" => format!("(self.add_time as i64) + parts.next().and_then(|s| if (s.trim_start().len() == 0) {{ Some({}) }} else {{s.parse::<i64>().ok() }}).unwrap_or_else( \
       || {{ println!(\"Expect to parse i64 from TUS for col[{},{},{}]\");  {} }} )",NAI64,ix,jx,nm,NAI64),
    "str"|"Str"|"STR" => format!("parts.next().and_then(|s| Some(s.trim_start()) ).unwrap_or(\"\") "),
    "D102"|"d102" => format!("parts.next().and_then(|s|  if s.trim_start().len() == 0 {{ Some({}) }} else {{ Some({{  \
                                           let mut ss = s.trim_start().split('.'); let x0 = 100 * ss.next().unwrap_or(\"0\").parse::<i64>().unwrap_or(0 as i64); \
                                           let mut cs1 = ss.next().unwrap_or(\"\").chars(); \
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * 10 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64) \
                                    }})}}).unwrap_or_else( || {{println!(\"rb{{}}{{}}_t{{}} in_i={{}}: Expect to parse D102 for col[{},{},{}]: |{{}}|\", \
                                    self.dc,self.mtype,self.thread_i,in_i,&line); {}}})", NAD102, ix,jx,nm, NAD102),
    "D142"|"d142" => format!("parts.next().and_then(|s|  if s.trim_start().len() == 0 {{ Some({}) }} else {{ Some({{  \
                                           let mut ss = s.trim_start().split('.'); let x0 = 100 * ss.next().unwrap_or(\"0\").parse::<i64>().unwrap_or(0 as i64); \
                                           let mut cs1 = ss.next().unwrap_or(\"\").chars(); \
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * 10 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64) \
                                    }})}}).unwrap_or_else( || {{println!(\"Expect to parse D142 for col[{},{},{}]: |{{}}|\",line); {} }})", NAD142, ix,jx,nm, NAD142),
    "D185"|"d185" => format!("parts.next().and_then(|s|  if s.trim_start().len() == 0 {{ Some({}) }} else {{ Some({{  \
                                           let mut ss = s.trim_start().split('.'); let x0 = (100000 as i64) * ss.next().unwrap_or(\"0\").parse::<i64>().unwrap_or(0 as i64); \
                                           let mut cs1 = ss.next().unwrap_or(\"\").chars(); \
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * (10000 as i64) \
                                              + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64) * (1000 as i64) \
                                              + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * (100 as i64) \
                                              + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64) * (10 as i64) \
                                              + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) \
                                    }})}}).unwrap_or_else( || {{println!(\"Expect to parse D185 for col[{},{},{}]\"); {} }})", NAD185, ix,jx,nm, NAD185),
    _ => "BADDATA".to_string()

  };
  return match typ {
    "str"|"STR"|"Str" => {  TokenStream2::from_str(format!("self.v_{:02}_{:02}_{}.push_str({}); self.v_{:02}_{:02}_{}.push(\'|\')", 
           ix, jx,nm, parsing_string, ix, jx,nm).as_str()).expect(&format!("gen_csv_part_read failed parse: {},{},{},{}",ix,jx,nm,typ))
    },
    _ => {TokenStream2::from_str(format!("self.v_{:02}_{:02}_{}[self.i_write] = {}", 
     ix, jx,nm, parsing_string).as_str()).expect(&format!("gen_csv_part_read failed parse: {},{},{},{}",ix,jx,nm,typ))
    }
  }
}
/*
    let mut parts = line.split(',');
    let l0_c:char = parts.next().and_then(|s| Some(s.chars().nth(0).unwrap_or('X'))).unwrap_or('X');
    let l1_i:i64 = parts.next().and_then(|s| s.parse::<i64>().ok()).unwrap_or_else( || {println!("Expected element 1 to be integer"); -1});
    let l2_i:i64 = parts.next().and_then(|s| s.parse::<i64>().ok()).unwrap_or_else( || {println!("Expected element 1 to be integer"); -1});
    let l3_sym = parts.next().and_then(|s| Some(s) ).unwrap_or("");
    let l4_i:i64 = parts.next().and_then(|s| s.parse::<i64>().ok()).unwrap_or_else( || {println!("Expected element 1 to be integer"); -1});
    let l5_i:i64 = parts.next().and_then(|s|  Some({ 
                                           let mut ss = s.split('.'); let x0 = 100 * ss.next().unwrap_or("0").parse::<i64>().unwrap_or(0 as i64);
                                           let mut cs1 = ss.next().unwrap_or("").chars();
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * 10 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64)
                                    })).unwrap_or_else( || {println!("Expected to get l5d but did not."); -1});
*/
/*
    "d185"|"D185" => format!("(match <i64>::try_from(part[{}].split('.').next()) { Ok(val)=>val*10000,Err(_e)=>0 }) \
                            + (part[{}].split('.').next().next().chars().nth(0).to_digit(10).unwrap_or(0) * 10000 \
                            + (part[{}].split('.').next().next().chars().nth(1).to_digit(10).unwrap_or(0) * 1000 \
                            + (part[{}].split('.').next().next().chars().nth(2).to_digit(10).unwrap_or(0) * 100 \
                            + (part[{}].split('.').next().next().chars().nth(3).to_digit(10).unwrap_or(0) * 10 \
                            + (part[{}].split('.').next().next().chars().nth(4).to_digit(10).unwrap_or(0) ",ix),
 */
impl RTbSchema {
  pub fn new(od: &Vec<Vec<String>>, on_dc: &String, on_mtype: &String) -> RTbSchema {

    let dc = on_dc.chars().nth(0).expect("on_dc should have 1 chars");
    let mtype = on_mtype.chars().nth(0).expect("on_mtype should have at least 1 char");
    let mut cntK:usize = 0;
    for ii in 0..od.len() {
      let ii = ii as usize;
      if (od[ii][0].len() <= 0) || (od[ii][1].len() <= 0) {
      } else if (od[ii][0].chars().nth(0).expect("od[ii][0] should have char") == dc) && 
                (od[ii][1].chars().nth(0).expect("od[ii][1] should have char") == mtype) {
        cntK = cntK +1;
      }
    }
    let tlen = cntK;  
    let mut v_01_col = Vec::<String>::new(); 
    let mut v_02_coltype = Vec::<String>::new(); 
    let mut v_03_vloc = Vec::<i8>::new();
    let mut v_04_origloc = Vec::<i8>::new();
    let mut on_origloc = 0;
    for ii in 0..od.len() {
      let ii = ii as usize;

      if (od[ii][0].len() <= 0) || (od[ii][1].len() <= 0) {
      } else if (od[ii][0].chars().nth(0).expect("od[ii][0] no char?") == dc) && 
                (od[ii][1].chars().nth(0).expect("od[ii][1] no char?") == mtype) {
        v_01_col.push(od[ii][2].clone());
        v_02_coltype.push(od[ii][3].clone());  v_03_vloc.push(  match od[ii][4].parse::<i8>() { Ok(val)=>val,Err(_e)=>-5 });
        v_04_origloc.push(on_origloc); on_origloc = on_origloc+1;
      }

    }
    return RTbSchema{tlen:tlen, dc:dc, mtype:mtype, v_01_col:v_01_col, v_02_coltype:v_02_coltype,
       v_03_vloc:v_03_vloc, v_04_origloc:v_04_origloc};

  }
  pub fn emit_read_buffer(&self) -> TokenStream2 {
     let dc = self.dc;  let mtype = self.mtype; 
     let ReadBufferName = format!("ReadBuffer{}{}",dc,mtype);
     let ReadBufferName = TokenStream2::from_str(&ReadBufferName.trim_matches('"')).expect("ReadBufferName should be parseable");
     let qnewline = TokenStream2::from_str("\n").expect("Trying to eliminate a printout string");
     let mut col_declares:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     let mut n_cols = 0;
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] < 0 {
       } else if self.v_02_coltype[ix] == "str" || self.v_02_coltype[ix] == "STR" || self.v_02_coltype[ix] == "Str" {
         let nt =  format!("pub v_{:02}_{:02}_{}:String", ix, self.v_03_vloc[ix],self.v_01_col[ix] );
         col_declares.push(TokenStream2::from_str(&nt.as_str()).expect("Failed to parse col_declars"));
         n_cols += 1;
       } else {
         let nt =  format!("pub v_{:02}_{:02}_{}:Vec::<{}>", ix, self.v_03_vloc[ix],self.v_01_col[ix],map_rust_type!(self.v_02_coltype[ix]) );
         //let nt:TokenStream = nt.parse_str().expect(&format!("Parsing emit_read_buffer failed on ix={}",ix));
         col_declares.push(TokenStream2::from_str(&nt.as_str()).expect("Failed to parse col_declars"));
         n_cols += 1;
       }
     }
     let n_col_text = TokenStream2::from_str(format!("{}", n_cols).as_str()).expect("n_col_text");
     col_declares.push(TokenStream2::from_str(&format!("pub v_xx_{:02}_origi:Vec::<{}>", n_cols, ORIG_TYPE_TEXT)).expect("pub v_xx col_declares")); 
     let mut col_new_vecs:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] < 0 { 
         // Push no Token Skip!
         //col_parsevecs.push(TokenStream2::from_str(&"let dummy = parts.next();").expect("Really expectged basic parts next to parse."));
       } else if self.v_02_coltype[ix] == "str" || self.v_02_coltype[ix] == "STR" || self.v_02_coltype[ix] == "Str" {
         let nt = format!("v_{:02}_{:02}_{}:String::with_capacity(5*in_bufflen)",
            ix, self.v_03_vloc[ix],self.v_01_col[ix]);
         col_new_vecs.push(TokenStream2::from_str(&nt.as_str()).expect(&format!("Parsing vec create emit_readbuffer failed on ix={}",ix)));
       } else {
         let nt = format!("v_{:02}_{:02}_{}:vec![{};in_bufflen]",
            ix, self.v_03_vloc[ix],self.v_01_col[ix], if ix == 0 { format!("b'{}'", self.mtype) } else { map_rust_default!(self.v_02_coltype[ix])});
         //let nt:TokenStream = nt.parse().expect(&format!("Parsing vec create emit_readbuffer failed on ix={}",ix));
         //println!(" On ix = {}  we have nt = {}", ix, nt);
         col_new_vecs.push(TokenStream2::from_str(&nt.as_str()).expect(&format!("Parsing vec create emit_readbuffer failed on ix={}",ix)));
       }
     }
     col_new_vecs.push(TokenStream2::from_str(&format!("v_xx_{:02}_origi:vec![0 as {};in_bufflen]", n_cols, ORIG_TYPE_TEXT)).expect("col_new_vecs_orig"));
     let mut col_clone_vecs:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] < 0 { 
       } else {
         let nt = format!("v_{:02}_{:02}_{}:self.v_{:02}_{:02}_{}.clone()",
            ix, self.v_03_vloc[ix],self.v_01_col[ix], ix, self.v_03_vloc[ix],self.v_01_col[ix]);
         col_clone_vecs.push(TokenStream2::from_str(&nt.as_str()).expect(&format!("Parsing vec clone create emit_readbuffer failed on ix={}",ix)));
       }
     }
     col_clone_vecs.push(TokenStream2::from_str(&format!("v_xx_{:02}_origi:self.v_xx_{:02}_origi.clone()", n_cols, n_cols)).expect("col_clone_vecs_orig"));

     let mut v_clear_strings:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] >= 0 && (self.v_02_coltype[ix] == "STR" || self.v_02_coltype[ix]=="Str" || self.v_02_coltype[ix] == "str" ) {
         v_clear_strings.push(TokenStream2::from_str(format!("self.v_{:02}_{:02}_{}.clear();\n",ix,self.v_03_vloc[ix], self.v_01_col[ix]).as_str()).expect("clearstrings"));
       }
     }
     let mut dsp_on_quote:Vec<String> = Vec::new();  let mut dsp_v_qt:Vec<String> = Vec::new();
     for ix in 0..self.tlen {
        if self.v_03_vloc[ix] >= 0 {
          dsp_on_quote.push(format!("[{}:{}:{}:{}:{{}}]", ix, self.v_03_vloc[ix],self.v_01_col[ix],self.v_02_coltype[ix]));
          dsp_v_qt.push(match self.v_02_coltype[ix].as_str() {
            "C1"|"c1" =>   format!("format!(\"'{{}}'\", self.v_{:02}_{:02}_{}[iline] as char)", ix, self.v_03_vloc[ix],self.v_01_col[ix]),
            "C2"|"c2" =>   format!("format!(\"'{{}}{{}}'\", (self.v_{:02}_{:02}_{}[iline] as [u8;2])[0] as char, (self.v_{:02}_{:02}_{}[iline] as [u8;2])[1] as char)", 
               ix, self.v_03_vloc[ix],self.v_01_col[ix],
               ix, self.v_03_vloc[ix],self.v_01_col[ix]),
            "D102"|"d102" =>   format!("format!(\"{{}} aka ${{}}\", self.v_{:02}_{:02}_{}[iline], (self.v_{:02}_{:02}_{}[iline] as f64)/100.00)", 
               ix, self.v_03_vloc[ix],self.v_01_col[ix],
               ix, self.v_03_vloc[ix],self.v_01_col[ix]),
            "D185"|"d185" =>   format!("format!(\"{{}} aka ${{}}\", self.v_{:02}_{:02}_{}[iline], (self.v_{:02}_{:02}_{}[iline] as f64)/100000.00)", 
               ix, self.v_03_vloc[ix],self.v_01_col[ix],
               ix, self.v_03_vloc[ix],self.v_01_col[ix]),
            "STR"|"str" => format!("format!(\"\\\"{{}}\\\"\", self.v_{:02}_{:02}_{}.split('|').nth(iline).expect(&format!(\"no {{}} of {:02}_{:02}_{}\",iline)))", 
               ix, self.v_03_vloc[ix],self.v_01_col[ix], ix, self.v_03_vloc[ix],self.v_01_col[ix]),
            _ => format!("self.v_{:02}_{:02}_{}[iline]", ix, self.v_03_vloc[ix],self.v_01_col[ix])
          });
          if ix == 0 {
            dsp_v_qt[ix] = "self.mtype".to_string();
          }
        }
     }
     dsp_on_quote.push(format!("[origi:{{}}]").to_string());
     dsp_v_qt.push(format!("self.v_xx_{:02}_origi[iline]",n_cols));
     let t_quote =  TokenStream2::from_str(format!("println!(\"Thread:{{}}::{{}}:{}\",self.thread_i, iline,{});",  dsp_on_quote.join(","),dsp_v_qt.join(",")).as_str()).expect("DisplayFail Parse");
     let mut csv_parsing:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     //for ix in 0..self.tlen {
     for ix in 0..self.tlen {
       let mut to_read = gen_csv_part_read(ix as i8, self.v_03_vloc[ix], &self.v_01_col[ix], &self.v_02_coltype[ix]);
       csv_parsing.push(to_read);
     }
     csv_parsing.push(TokenStream2::from_str(format!("self.v_xx_{:02}_origi[self.i_write] = in_i as {}", n_cols, ORIG_TYPE_TEXT).as_str()).expect("csv_parsing origi"));
     let csv_parsing_first = csv_parsing[0].clone();
     let no_0_csv_parsing:Vec<TokenStream2> = csv_parsing.iter().skip(1).cloned().collect();
     let in_i_parse = TokenStream2::from_str(&format!("in_i: {}", ORIG_TYPE_TEXT)).expect("in_i_parse");


     let mut field_builder:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] >= 0 {
          field_builder.push(TokenStream2::from_str( 
            match self.v_02_coltype[ix].as_str() {
               //"D102"|"d102"|"d142"|"D142"|"D182"|"d182"|"c2"|"C2"|"C1"|"c1" =>
               //format!("Field::new(\"{}\",DataType::Int32,false)", self.v_01_col[ix]),
               _ => format!("Field::new(\"{}\", {}, false)", self.v_01_col[ix], map_arrow_type![self.v_02_coltype[ix]]) 
            }.as_str()).expect(&format!("field_builder collapsed on ix={}",ix)));
       }
     }
     field_builder.push(TokenStream2::from_str(format!("Field::new(\"origi\",{},false)", map_arrow_type![ORIG_TYPE_TEXT.to_string()]).as_str()).expect("origi field_builder"));
     field_builder.push(TokenStream2::from_str("Field::new(\"thread_i\",arrow::datatypes::DataType::Int8,false)").expect("origi field_builder"));
     let no_0_field_builder:Vec<TokenStream2> = field_builder.iter().skip(1).cloned().collect();

     let mut arrow_fields:Vec<TokenStream2> = Vec::<TokenStream2>::new();
     for ix in 0..self.tlen {
       if self.v_03_vloc[ix] >= 0 {
         arrow_fields.push(gen_field_array_type(ix as i8,self.v_03_vloc[ix],&self.v_01_col[ix],&self.v_02_coltype[ix]));
       }
     }
     arrow_fields.push(TokenStream2::from_str(format!("Arc::new({}::from( (self.v_xx_{:02}_origi[0..self.i_write]).to_vec())) as ArrayRef",
         map_arrow_array_type![ORIG_TYPE_TEXT.to_string()], n_cols).as_str()).expect("origi arrow_fields"));
     arrow_fields.push(TokenStream2::from_str("Arc::new(arrow::array::Int8Array::from( vec![ (self.thread_i as i8); self.i_write] )) as ArrayRef").expect("thread_i arrow_fields"));
     let no_0_arrow_fields:Vec<TokenStream2> = arrow_fields.iter().skip(1).cloned().collect();
     let make_dc_mtype = TokenStream2::from_str(format!("dc:\"{}\",mtype:\"{}\",",dc,mtype).trim_matches('"')).expect("make_dc_mtype parsing");

     //println!("-------------------------------------------");
     //println!("--- Emitter, {},{} let's look at the arrow fields:", dc,mtype);
     //println!("{}", quote!{ #(#arrow_fields),*});
   
     let writer_clone_ts:TokenStream2 = TokenStream2::from_str( 
        (if ARROW_IPC_OR_PARQUET == 0 {"writer_clone: Option<std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>>>"
         } else { "writer_clone: Option<std::sync::Arc<std::sync::Mutex<parquet::arrow::arrow_writer::ArrowWriter<File>>>>" })).expect("ArrowType Readerbuffer.");
     let arrow_type_ts:TokenStream2 = TokenStream2::from_str(
        (if  ARROW_IPC_OR_PARQUET == 0 { "arrow_ipc::writer::FileWriter<File>" } else { "parquet::arrow::arrow_writer::ArrowWriter<File>" } )
        ).expect("ArrowType ReadBuffer 2");
     //let some_arrow_fields:Vec<TokenStream2> = arrow_fields.into_iter().take(3).collect();
     let out =  quote!{
       pub struct #ReadBufferName {
         pub dc: &'static str, pub mtype: &'static str, pub thread_i:i8, pub add_time:i64,
         pub n_cols:u8,
         pub n_bufflen: usize,
         pub i_write: usize, pub skip_m_type:bool,
         #(#col_declares),*,
         #writer_clone_ts ,
         pub schema: arrow_schema::Schema
       }
       #qnewline
       impl Clone for #ReadBufferName {
         fn clone(&self) -> Self {
            return #ReadBufferName { #make_dc_mtype thread_i:self.thread_i, add_time:self.add_time, n_cols:self.n_cols, 
              n_bufflen:self.n_bufflen, i_write:self.i_write, skip_m_type: self.skip_m_type, #(#col_clone_vecs),*,
              writer_clone: match &self.writer_clone { Some(x)=>Some(x.clone()),None=>None}, schema: self.schema.clone() }
         }
       }
       impl #ReadBufferName {
          pub fn new(in_bufflen:usize, in_thread_i:i8, add_time:i64, skip_m_type: bool) -> #ReadBufferName {
            let fields = if skip_m_type { vec![#(#no_0_field_builder),*] } else { vec![ #(#field_builder),* ] };
            return #ReadBufferName {#make_dc_mtype thread_i: (in_thread_i as i8), add_time:add_time, n_cols: #n_col_text,
              n_bufflen:in_bufflen as usize, i_write:0 as usize, skip_m_type: skip_m_type, #(#col_new_vecs),*, 
              writer_clone: None, schema:arrow_schema::Schema::new(fields)}
          }

          fn partial_clone(&self, thread_i:i8) -> Self {
            return #ReadBufferName { #make_dc_mtype thread_i:thread_i, add_time:self.add_time, n_cols:self.n_cols, 
              n_bufflen:self.n_bufflen, i_write:self.i_write, skip_m_type: self.skip_m_type, #(#col_clone_vecs),*,
              writer_clone: match &self.writer_clone { Some(x)=>Some(x.clone()),None=>None}, schema: self.schema.clone() }
          }
          pub fn parse_one_line(&mut self, line:&String, #in_i_parse) -> i32 {
            let mut parts = line.split(',');
            //#(#no_0_csv_parsing);*;
            if self.i_write >= self.n_bufflen { self.flush_current_data(); }
            if self.skip_m_type {
              let _dunder = parts.next().expect("I don't care about dunder");
            } else {
              #csv_parsing_first;
            }
            #(#no_0_csv_parsing);*;
            self.i_write = self.i_write + (1 as usize);
            return match <i32>::try_from(self.i_write) { Ok(val) => val, Err(_e) => -6 as i32 }
          }
          pub fn consider_parse_one_line(&mut self, line:&String, #in_i_parse) -> i32 {
            let mut i_record: i32 = 0;
            if line.len() == 0 {  return - 2; }
            let first_word = (*line).split(",").next().expect("first word of line");
            if first_word == self.mtype {
               i_record = self.parse_one_line(line, in_i);
            }
            //if self.i_write >= self.n_bufflen {
            //  self.flush_current_data();
            //}
            return i_record
          }
          pub fn attack_one_line(&mut self, line:String, #in_i_parse) -> i32 {
            if self.i_write >= self.n_bufflen { self.flush_current_data(); }
            let mut parts = line.split(',');
            if let Some(o_first_word) = parts.next() {
               let first_word = o_first_word;//.unwrap();
               if (first_word.len() <= 0) || (first_word != self.mtype) {
                return(0);
               }
            }
            #(#no_0_csv_parsing);*;
            self.i_write = self.i_write + (1 as usize);
            if self.i_write >= self.n_bufflen { self.flush_current_data(); }
            return 1 
          }
          pub fn process_all_lines_and_retreat(& mut self, mut_reader: & mut std::io::BufReader<File>, max_bytes: u64) -> u32 {
            let mut lines_iter = mut_reader.lines(); let mut read_bytes:u64 = 0 as u64; 
            let mut in_i:u32 = 0; let mut my_i:u32 = 0;
            while let Some(line) = lines_iter.next() {
              let line = line.expect(&format!("error reading line  {} for bytes={}/{}",in_i, read_bytes, max_bytes));
              let llen = match <u64>::try_from(line.len()) { Ok(val)=>val, Err(_e)=>0 };  
              let out_x = self.attack_one_line(line, in_i); in_i = in_i + 1; 
              if out_x >= 1 { my_i = my_i + 1; }
              read_bytes = read_bytes + llen; 
              if read_bytes >= max_bytes { break; }
            } 
            mut_reader.seek_relative(-match <i64>::try_from(read_bytes+1) { Ok(val)=>val,Err(_e)=>0});
            self.flush_current_data();
            return my_i 
          }
          pub fn set_writer_clone(& mut self, in_writer_clone: 
            Option<std::sync::Arc<std::sync::Mutex< #arrow_type_ts >>>  
          ) {
          //pub fn set_writer_clone(& mut self, in_writer_clone: Option<std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>>>  ) {
             self.writer_clone = match in_writer_clone { Some(x) => Some(x.clone()), None=>None };
          }
          pub fn display_line(&self, iline:usize) {
            #t_quote
          }
          // Make complete Reader finish
          //pub fn finish(& mut self) {
          //   self.writer.finish().expect(&format!("Expected to finish writer but failed for {},{},{}", self.dc, self.mtype,self.i_write));
          //   println!("  -- Finished on file for {},{} ", self.dc, self.mtype);
          // }
          pub fn flush_current_data(& mut self) {
            if self.i_write <= 0 {
              return;
            }
            {let arrow_fields = if self.skip_m_type { vec![#(#no_0_arrow_fields),*] } else { vec![#(#arrow_fields),*] };
            let batch = arrow::record_batch::RecordBatch::try_new(self.schema.clone().into(),arrow_fields
              ).expect(&format!("Arrow Recordbatch attempt: {},{}",self.dc,self.mtype));
            //println!(" -- Trying to get writer guard for {},{}, lines written={}, thread_given is {}", self.dc,self.mtype,self.i_write, self.thread_i);
            // Lock unwrapping: Here, in case mutex is poisoned we diagnose an error at unlock.
            let mut err_state = 0;
            match &self.writer_clone { 
              Some(x) => { 
                           match x.lock() {
                             Ok(mut wg) => {
                               //println!(" ---- Hey I think we achieved an unwrap of lock for thread_i = {}, {}{}", self.thread_i, self.dc, self.mtype);
                               wg.write(&batch).expect(&format!("Failed writing the batch for {},{},{}", self.dc,self.mtype,self.i_write));
                               //println!(" ---- Finished Writing for thread {}.", self.thread_i);
                             },
                             Err(e) => {
                               println!("Error on trying to write a completion for thread_i={},{}{}", self.thread_i, self.dc, self.mtype);
                               err_state = 1;
                             }
                           }
                         },
              None => {} 
            }
            if err_state > 0 { self.writer_clone = None; }
            }
            #(#v_clear_strings)*
            self.i_write = 0;
          }
          //pub fn clear_strings(& mut self) {
          //  #(#v_clear_strings)*
          //}
        }
    };
    /*
          pub fn write_to_arrow_file(& mut self ) {
            let arrow_fields = vec![#(#arrow_fields),*];
            // Note try_new needs some crazy cast of self.schema
            let batch = arrow::record_batch::RecordBatch::try_new(self.schema.clone().into(),arrow_fields
              ).expect(&format!("Arrow Recordbatch attempt: {},{}",self.dc,self.mtype));
            //let batch = RecordBatch::try_new(
            //  self.schema.clone(),  vec![  #(#arrow_fields),*  ] ).expect("we were trying to write to arrow");
            //let mut file = File::create(format!("c://users/alanj/Dropbox/py/ptc/save_data/output{}{}.arrow_zstd19",self.dc,self.mtype)).expect("got a file");
            let mut file = OpenOptions::new().read(true).write(true).append(true).open(
              format!("c://users/alanj/Dropbox/py/ptc/save_data/output{}{}.arrow_zstd19",self.dc,self.mtype)
            ).expect(&format!("Opened the File or failed? : {} ", format!("c://users/alanj/Dropbox/py/ptc/save_data/output{}{}.arrow_zstd19",self.dc,self.mtype)));
            let compressor = zstd::bulk::Compressor::new(19).expect("can use default compression level first set to 19");
            //let mut compression_context = arrow_ipc::compression::CompressionContext::default();
            let write_options = arrow_ipc::writer::IpcWriteOptions::try_new(8,false,arrow_ipc::MetadataVersion::V5
              ).expect("write_opentions IpcWriteOption fail on try_new").try_with_compression(
              Some(arrow_ipc::CompressionType::ZSTD)).expect("write_options fail on try_with_compression");
            //let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(& mut file, &self.schema, write_options).expect("Is Writer constructed?");
            let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(& mut file, &self.schema, write_options).expect("Is Writer constructed?");
            //let compressor = arrow_ipc::compression::Compressor::new(arrow_ipc::compression::CompressionCodec:Zstd, Some(19)).expect("Zstd compressor build fail");
            //let compression_type = arrow_
            //let compression_context = arrow_ipc::compression::CompressionContext::new(compressor);
            //compression_context.set_zstd_parameter(zstd::zstd_safe::CParameter::CompressionLevel(19))?;
            //let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(& mut file, &self.schema
            //let mut writer = arrow_ipc::writer::FileWriter::new(file).with_compression(Some(IpcCompression::ZSTD)).with_parallel(false).batched(self.schema);
            //let write_options = arrow_ipc::writer::IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::writer::IpcCompression::ZSTD))?;
            //let write_options = arrow_ipc::writer::IpcWriteOptions::new().try_with_compression(Some(arrow_ipc::Message::CompressionType::ZSTD));
            //let write_options = arrow_ipc::writer::IpcWriteOptions {
            //  batch_compression_type: Some(arrow_ipc::Message::CompressionType:ZSTD),
            //  compression_context: Some(compression_context),
            //  ..Default::default()
            // };
             //let mut writer = FileWriter::try_new_with_options(file, &*self.schema, write_options)?;
             writer.write(&batch).expect(&format!("Writing the batch finally to data {}{} {}", self.dc,self.mtype,self.i_write));
             writer.finish().expect(&format!("Expected to finish fillling of a writer {};{},{}", self.dc,self.mtype,self.i_write));
             println!("In our test we have now written to output.arrow_zstd19");
             self.i_write = 0;
          }
       }

     };
     */
     //let out = quote!{  println!("Bad Qute!"); };
     out
  }
}
// End of RTbschema definition
//

///~///////////////////////////////////////
/// writer_loc structure/object
///
/// Contains logic and primary save point for a mutex to the file writer.
///  This way all parallel threads are essentially borrowing from single location
struct FakeWriter {
  pub faketxt: & 'static str
}
impl FakeWriter {
  pub fn new() -> FakeWriter {
    return FakeWriter{faketxt: "faketxt"}
  }
  fn gen_writer_loc_text(&self) ->TokenStream2 {
   let arrow_type_ts = TokenStream2::from_str(
     if ARROW_IPC_OR_PARQUET == 0 { "arrow_ipc::writer::FileWriter<File>" } else { "parquet::arrow::arrow_writer::ArrowWriter<File>" } 
     ).expect("arrow_type_ts:FakeWriter");
   let writer_loc_struct_def =quote![ 
     pub struct WriterLoc {
        pub dc: String, pub mtype: String, pub save_dir: String, pub schema: arrow_schema::Schema,
        pub want_fn: String, 
        //pub write_options: arrow_ipc::writer::IpcWriteOptions,
        //pub write_loc: std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>>,
        pub write_loc: std::sync::Arc<std::sync::Mutex< 
         #arrow_type_ts 
        >>,
        pub i_am_original:bool
     }
     ];
    let writer_loc_clone_def = quote![
     impl Clone for WriterLoc {
       fn clone(&self) -> WriterLoc {
         return WriterLoc {dc:self.dc.clone(),mtype:self.mtype.clone(), save_dir:self.save_dir.clone(), schema:self.schema.clone(),
           want_fn:self.want_fn.clone(), //write_options:self.write_options.clone(), 
           write_loc: self.write_loc.clone(), i_am_original:false
         }
       }
     }
    ];
    //let print_new_text:TokenStream2 = TokenStream2::from_str(format!(
    //   "println!(\"(writer_loc{{}},{{}} - in_save_dir={{}}) -- we initiate, hope it works.\",in_dc.clone(),in_mtype.clone(),in_save_dir);").as_str()).expect("pnt");
    let want_fn_dec_text:TokenStream2 = TokenStream2::from_str(format!(
       "let want_fn = format!(\"{{}}/rbc_{{}}{{}}_zstd{}\", in_save_dir.clone(), in_dc.clone(),in_mtype.clone());",
        if ARROW_IPC_OR_PARQUET==0 { ".arrow" } else {"19.parquet"} ).as_str()).expect("wantfn");
    let zstd_level_text = TokenStream2::from_str(format!("{}",ZSTD_LEVEL).as_str()).expect("zstd_text");
    let arrow_opener_ts = 
      if ARROW_IPC_OR_PARQUET == 0 {
        quote![
         let write_options = arrow_ipc::writer::IpcWriteOptions::try_new(8,false,arrow_ipc::MetadataVersion::V5 
              ).expect("write_opentions IpcWriteOption fail on try_new").try_with_compression( 
              Some(arrow_ipc::CompressionType::ZSTD)).expect("write_options fail on try_with_compression");
              //compression_type).expect("write_options fail on try_with_compression");
         let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(opened_file,
            &in_schema.clone(), write_options.clone()).expect(&format!("Opened the file writer or maybe failed {}{}",in_dc.clone(),in_mtype.clone()));
        ]
      } else {
        quote![
         let mut opened_file = File::create(want_fn.clone()).expect(&format!("Hope to open file {}.", want_fn.clone()));
         let writer_properties = parquet::file::properties::WriterProperties::builder()
          .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new((#zstd_level_text as i32)).expect("ZSTD Level 19 should exist")))
          .set_writer_version(WriterVersion::PARQUET_2_0)
          .build();
         let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(opened_file, Arc::new(in_schema.clone()) 
           as arrow_schema::SchemaRef, Some(writer_properties)).expect(&format!(" Failed to generate writer for {}{}",  in_dc.clone(), in_mtype.clone()));
        ]
      };
    let writer_loc_imp_def = quote![
     impl WriterLoc {
       pub fn new(in_dc: String, in_mtype:String, in_save_dir:String, in_schema: arrow_schema::Schema) -> WriterLoc {
         //#print_new_text
         #want_fn_dec_text
         println!("Creating a file to write to of name {}", &want_fn);
         // Compression choices for Arrow ipc seem to be gone
         //let compression_level = arrow_ipc::compression::ZstdLevel::try_new(3).unwrap(); // Choose a compression level (1-22)
         //let compression_type = Some(arrow_ipc::compression::CompressionType::ZSTD(compression_level));
         //
         let mut opened_file = File::create(want_fn.clone()).expect(&format!("Hope to open file {}.", want_fn.clone()));
         #arrow_opener_ts
         let write_loc = std::sync::Arc::new(std::sync::Mutex::new(writer));
         return WriterLoc{ dc:in_dc.clone(), mtype: in_mtype.clone(), save_dir: in_save_dir.clone(), schema: in_schema.clone(),
           want_fn: want_fn, //write_options:write_options, 
           write_loc: write_loc, i_am_original:true
         }
       }

       pub fn give_mutex_to_a_thread(&mut self) -> Option<std::sync::Arc<std::sync::Mutex< #arrow_type_ts>>> {
       //pub fn give_mutex_to_a_thread(&mut self) -> Option<std::sync::Arc<std::sync::Mutex<arrow_ipc::writer::FileWriter<File>>>> {
           Some(std::sync::Arc::clone(&self.write_loc))
        }
       pub fn finish(& mut self) {
         // MUTEX Error state detection:
         //
         // Note we want only original lock holder to have power to finish files and close locks
         if self.i_am_original == false { return; }
         match self.write_loc.lock() {
            Ok(mut writer) => { writer.finish(); self.i_am_original = false; },
            Err(e) => {println!("writer_loc trying to finish but received an error");  println!("{}",e); }
         }
       }
     }
     impl Drop for WriterLoc {
       fn drop(& mut self) {
         if self.i_am_original {
           self.finish();
         }
       }
     }
   ]; 
   let writer_loc_def = quote![
      #writer_loc_struct_def
      #writer_loc_clone_def
      #writer_loc_imp_def
   ];
   return writer_loc_def;
  }
}
/* // text for arrow fields
             let file = File::create("output.arrow_zstd19")?;
            // 4. Configure compression with level 19
            let mut compression_context = CompressionContext::new();
            compression_context.set_zstd_parameter(CParameter::CompressionLevel(19))?;
            let write_options = IpcWriteOptions {
              compression: Some(IpcCompression::ZSTD),
              compression_context: Some(compression_context),
              ..Default::default()
             };

             // 5. Initialize the IPC file writer with compression options
             let mut writer = FileWriter::try_new_with_options(file, &*self.schema, write_options)?;

             // Write the record batch to the file
             writer.write(&batch)?;
             writer.finish()?;
             println!("Successfully wrote data to output.arrow");
          }*
 */
impl fmt::Display for RTbSchema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let iv:Vec<usize> = (0..self.tlen).collect();
    let v_s_rows:Vec<String> = iv.into_iter().map(|ix| format!("   {:2}:{:8}|{:4}|{:2}|{:2}",ix, self.v_01_col[ix],self.v_02_coltype[ix],self.v_03_vloc[ix],self.v_04_origloc[ix])).collect();
    write!(f,"\nRbTable({},{},{}):\n----------------\n{}", self.dc,self.mtype,self.tlen,v_s_rows.join("\n"))
  }
}

#[proc_macro]
pub fn read_config_psv(input: TokenStream) -> TokenStream {
   //let file_path_lit = parse_macro_input!(input as LitStr);
   //let file_path_str = file_path_lit.value();
   //let file_path_str = "c0.psv";
   // Construct the absolute path to the CSV file
   //let cargo_manifest_dir = std::env::var("..\\..\\ptc\\test_src\\")
   //        .expect("Looked for ../../ptc/test_src?  CARGO_MANIFEST_DIR not set");
   //let base_path = PathBuf::from(cargo_manifest_dir);
   let file_path_str = format!("{}",input.to_string()).replace("\"","");
   let base_path = PathBuf::from("../test_src/");
   let full_path = base_path.join(&file_path_str);
   println!("Attempting to open a file in {}", full_path.display());
   let mut file = File::open(&full_path)
       .expect(&format!("Could not open PSV file: {}", full_path.display()));

    let mut contents = String::new();
    file.read_to_string(&mut contents)
            .expect("Could not read PSV file contents");
    
    // Process the CSV data using the csv crate
    let mut rdr = csv::ReaderBuilder::new().has_headers(false)
        .delimiter(b'|') // Set delimiter to semicolon (as a byte)
        .from_reader(contents.as_bytes());
        //Reader::from_reader(contents.as_bytes()).delimiter(b'|');
    let mut records_data:Vec<Vec<String>> = Vec::new();
    for result in rdr.records() {
      let record = result.expect("Error reading CSV record");
      let fields: Vec<String> = record.iter().map(|s| (s).to_string()).collect();
      records_data.push(fields);
    }
    let ii:usize = 0 as usize;
    println!("read_config_psv we got {} records with the first record being {},{},{},{}",
      records_data.len(), &(records_data[0])[0], &(records_data[0])[1],
        &(records_data[0])[2], &(records_data[0])[3]);
    std::io::stdout().flush().unwrap();
    let unique_dc_set: HashSet<String> = records_data.clone().into_iter().map(|e|return e[0].clone()).collect();
    let mut unique_dc_set:Vec<String> = unique_dc_set.into_iter().collect();
    unique_dc_set.sort();
    let unique_dc_set = unique_dc_set;  // fix in place
    println!("read_config_psv the unique set of values[0]  turns out to be ({}). ", unique_dc_set.join(","));
    
    let unique_mtype_set: HashSet<String> = records_data.clone().into_iter().map(|e|return e[1].clone()).collect();
    let mut unique_mtype_set:Vec<String> = unique_mtype_set.into_iter().collect();
    unique_mtype_set.sort();


    let unique_coltype_set: HashSet<String> = records_data.clone().into_iter().map(|e|return e[3].clone()).collect();
    let mut unique_coltype_set:Vec<String> = unique_coltype_set.into_iter().collect();
    unique_coltype_set.sort();
    let my_use_requirements: Vec<TokenStream2> = get_use_requirements(unique_coltype_set);


    println!("read_config_psv the unique set of values[1]  turns out to be ({}). ", unique_mtype_set.join(","));
    let mut v_rbs:Vec<RTbSchema> = Vec::<RTbSchema>::new();
    let mut str_dcs:String = format!("dcs :["); let mut str_mtypes:String = format!("mtypes :[");
    let mut str_qbmake:String = "let mut v_writer_locs = Vec::<WriterLoc>::new();\n".to_string();
    let mut sx_parser_quotes:Vec<TokenStream2> = Vec::<TokenStream2>::new();
    let mut sx_complete_parse_by_line:Vec<TokenStream2> = Vec::<TokenStream2>::new();
    let mut sx_complete_parse_by_type:Vec<TokenStream2> = Vec::<TokenStream2>::new();
    let mut sx_all_bufreader_by_type:Vec<TokenStream2> = Vec::<TokenStream2>::new();
    for ii in 0..unique_dc_set.len() {
      let mut sx_match:Vec<String> = Vec::<String>::new();
      let mut sx_match_by_line:Vec<String> = Vec::<String>::new();
      let mut sx_match_by_type:Vec<String> = Vec::<String>::new();
      let mut sx_set_by_type:Vec<String> = Vec::<String>::new();
      let mut n_mtypes_for_ii = 0;
      for jj in 0..unique_mtype_set.len() {
          let on_rb = RTbSchema::new(&records_data, &unique_dc_set[ii], &unique_mtype_set[jj]);
          if on_rb.tlen > 0 {
            str_dcs = format!("{}{} \"{}\"", str_dcs, if v_rbs.len() == 0 {""} else {","}, unique_dc_set[ii]);
            str_mtypes = format!("{}{} \"{}\"", str_mtypes, if v_rbs.len() == 0 {""} else {","}, unique_mtype_set[jj]);
            str_qbmake = format!("{}\nv_writer_locs.push(WriterLoc::new(\"{}\".to_string(),\"{}\".to_string(),save_dir.clone(),rb{}{}.schema.clone()));",
              str_qbmake, unique_dc_set[ii].to_lowercase(),unique_mtype_set[jj].to_lowercase(), unique_dc_set[ii].to_lowercase(),unique_mtype_set[jj].to_lowercase());
            sx_match.push(format!("Some('{}') => {{  self.rb{}{}.parse_one_line(&line, in_i)}}", unique_mtype_set[jj], 
               unique_dc_set[ii].to_lowercase(), unique_mtype_set[jj].to_lowercase()) );
            sx_match_by_line.push(format!("Some(\"{}\") => {{  v_lines_read[{}]=v_lines_read[{}]+1; self.rb{}{}.parse_one_line(&line, in_i)}}", unique_mtype_set[jj], 
               n_mtypes_for_ii, n_mtypes_for_ii, unique_dc_set[ii].to_lowercase(), unique_mtype_set[jj].to_lowercase()) );
            sx_match_by_type.push(format!("\"{}\" => {{ while let Some(line) = lines_iter.next() {{  \
              \n    let line = line.expect(\"Did we open the line in hard open?\"); \
              \n    let llen = match <u64>::try_from(line.len()) {{ Ok(val)=>val,Err(_e)=>0 }}; read_bytes = read_bytes + llen;          \
              \n    let read_x = self.rb{}{}.consider_parse_one_line(&line, in_i); \
              \n    if llen > 0 {{ in_i = in_i + 1; }} \
              \n    if read_bytes > max_bytes {{ break; }}\n    }} }}", unique_mtype_set[jj], unique_dc_set[ii].to_lowercase(), unique_mtype_set[jj].to_lowercase()));
            sx_set_by_type.push(format!("{} => {{out_vec[{}] = self.rb{}{}.process_all_lines_and_retreat(master_mut_reader, max_bytes);}}",
              n_mtypes_for_ii, n_mtypes_for_ii, unique_dc_set[ii].to_lowercase(),unique_mtype_set[jj].to_lowercase())); 
            v_rbs.push(on_rb); n_mtypes_for_ii = n_mtypes_for_ii+1;
          }
      }
      sx_match.push("Some(_) => { -1 }, None => { 0 }".to_string());
      sx_match_by_line.push("Some(_) => { -1 }, None => { 0 }".to_string());
      let sx_parser_text = format!("fn parse_one_line_{}(& mut self, line:&String, in_i:u32) -> i32 {{\n  return match line.chars().nth(0) {{\n{}\n  }}\n}} ",
        unique_dc_set[ii].to_lowercase(), sx_match.join(",\n"));
      sx_parser_quotes.push(TokenStream2::from_str(sx_parser_text.as_str()).expect(&format!("parsing parse_parser_quotes: dc={}:\n{}", unique_dc_set[ii], sx_parser_text)));
      let sx_complete_parse_by_line_text = format!("pub fn process_all_lines_{}(& mut self, file_clone:std::fs::File, max_bytes: u64) -> Vec<u32> \
           \n   {{ \
           \n      let mut reader = std::io::BufReader::with_capacity(self.buff_capacity as usize,file_clone); \
           \n      let start_offset = self.start_offset;
           \n      reader.seek(std::io::SeekFrom::Start(if start_offset == 0 {{ 0 }} else {{ start_offset-1}} )).expect(\"Failed to seek\"); \
           \n      let mut lines_iter = reader.lines(); \
           \n      let mut read_bytes:u64 = 0; let mut in_i:u32 = 0;   \
           \n      let mut v_lines_read:Vec<u32> = vec![0 as u32;{}]; \
           \n      if start_offset != 0 {{  \
           \n        if let Some(line) = lines_iter.next() {{ \
           \n           let line = line.expect(\"read first line fail \"); \
           \n           let llen = match <u64>::try_from(line.len()) {{ Ok(val)=>val,Err(_e)=>0 }}; 
           \n           read_bytes = read_bytes + llen -1;
           \n        }} \
           \n      }} \
           \n      while let Some(line) = lines_iter.next() {{ \
           \n        let line = line.expect(\"Parse in loop line read error \"); \
           \n        let llen = match <u64>::try_from(line.len()) {{ Ok(val)=>val,Err(_e)=>0}}; read_bytes = read_bytes + llen;      \
           \n        let read_x = match line.split(\",\").next()  {{\n{}\n}}; \
           \n        if llen > 0  {{ in_i = in_i + 1; }} \
           \n        if in_i % 500000 == 0  {{ println!(\"CompleteReader[thread={{}}] - read {{}} or [{{}}]\", self.thread_i, in_i, \
           \n             v_lines_read.iter().map(|x| format!(\"{{}}\",x)).collect::<Vec<String>>().join(\",\")); }} \
           \n        if read_bytes > max_bytes {{ break; }}\n}}\n  self.flush_all(); \n  return v_lines_read }}", 
              unique_dc_set[ii].to_lowercase(), n_mtypes_for_ii, sx_match_by_line.join(",\n"));
      sx_complete_parse_by_line.push(TokenStream2::from_str(sx_complete_parse_by_line_text.as_str()).expect(
              &format!("sx_complete_parse_by_line:\n{}",sx_complete_parse_by_line_text)));
      let sx_complete_parse_by_type_text = format!("pub fn process_all_lines_{}_only_type(& mut self, file_clone: std::fs::File, \
           \n      max_bytes: u64, in_type:String) -> u32 {{ \
           \n      let start_offset = self.start_offset;
           \n      let mut reader = std::io::BufReader::with_capacity(self.buff_capacity as usize, file_clone);
           \n      reader.seek(std::io::SeekFrom::Start(if start_offset == 0 {{ 0 }} else {{ start_offset-1}} )).expect(\"Failed to seek\"); \
           \n      let mut lines_iter = reader.lines(); \
           \n      let mut read_bytes:u64 = 0 as u64; let mut in_i:u32 = 0;   \
           \n      if start_offset != 0 {{  \
           \n        if let Some(line) = lines_iter.next() {{ \
           \n           let line = line.expect(\"Error reading first line \"); \
           \n           let llen = match <u64>::try_from(line.len()) {{ Ok(val)=>val, Err(_e)=>0 }};
           \n           read_bytes = read_bytes + llen -1; \
           \n        }} \
           \n      }} \
           \n      match in_type.as_str() {{\n{},_=>{{}}\n}}\n  \
           \n      self.flush_all(); \n \
           \n      return in_i }}", unique_dc_set[ii].to_lowercase(), sx_match_by_type.join(",\n"));
      sx_complete_parse_by_type.push(TokenStream2::from_str(sx_complete_parse_by_type_text.as_str()).expect(
        &format!("sx_complete_parse_by_type:\n{}",sx_complete_parse_by_type_text)));
      let sx_all_bufreader_by_type_text = format!("pub fn process_all_lines_by_type_{}(& mut self, master_mut_reader: &mut std::io::BufReader<File>, \
         \n      max_bytes: u64, mover:i8) -> Vec<u32> {{ \
         \n      let mut out_vec:Vec<u32> = vec![0 as u32; {} ]; \
         \n      for i_run in 0..{} {{
         \n         match (i_run+mover) % {} {{
         \n           {}, _=>{{}}  }} \
         \n      }}  \
         \n      return out_vec \n }}", unique_dc_set[ii].to_lowercase(), n_mtypes_for_ii, n_mtypes_for_ii, n_mtypes_for_ii, sx_set_by_type.join(",\n"));
      sx_all_bufreader_by_type.push(TokenStream2::from_str(sx_all_bufreader_by_type_text.as_str()).expect(
        &format!("sx_all_bufreader_by_type:\n{}", sx_all_bufreader_by_type_text)));
    }

    let qbmake:TokenStream2 = TokenStream2::from_str(str_qbmake.as_str()).expect(&format!("qbmake TokenStream2: error on {}",str_qbmake));

    let let_el_dcs_mtypes = format!("pub nrb: u8, pub dcs:[& 'static str;{}], pub mtypes:[& 'static str;{}]", v_rbs.len(), v_rbs.len());
    let mut little_element_names:Vec<String> =Vec::<String>::new();  let mut class_names:Vec<String> =Vec::<String>::new();
    let mut sx_el:Vec<String> = Vec::<String>::new(); sx_el.push(let_el_dcs_mtypes); 
       sx_el.push("pub start_offset: u64".to_string()); sx_el.push("pub thread_i:i8".to_string()); sx_el.push("pub save_dir:String".to_string());
       sx_el.push("pub add_time: i64".to_string()); sx_el.push("pub skip_m_type: bool".to_string());
       sx_el.push("pub buff_capacity: u64".to_string());  sx_el.push("pub verbose: i64".to_string()); 
    let mut sx_let:Vec<String> = Vec::<String>::new(); 
    let new_dcs_mtypes = format!("nrb:{} as u8,{}],{}]", v_rbs.len(),str_dcs.clone(), str_mtypes.clone());
    let mut sx_new:Vec<String> = Vec::<String>::new(); sx_new.push(new_dcs_mtypes.clone()); 
       sx_new.push("start_offset:start_offset".to_string()); sx_new.push("thread_i:thread_i".to_string()); sx_new.push("save_dir:save_dir.clone()".to_string());
       sx_new.push("add_time:add_time".to_string()); sx_new.push("skip_m_type:skip_m_type".to_string());
       sx_new.push("buff_capacity:buff_capacity".to_string());  sx_new.push("verbose:verbose".to_string()); 
    let mut sx_clone:Vec<String> = Vec::<String>::new(); sx_clone.push(new_dcs_mtypes.clone()); 
       sx_clone.push("start_offset:self.start_offset".to_string()); sx_clone.push("thread_i:self.thread_i".to_string());sx_clone.push("save_dir:self.save_dir.clone()".to_string());
       sx_clone.push("add_time:self.add_time".to_string()); sx_clone.push("skip_m_type:self.skip_m_type".to_string());
       sx_clone.push("buff_capacity:self.buff_capacity".to_string()); 
       sx_clone.push("verbose:self.verbose".to_string()); 
    let mut sx_partial_clone:Vec<String> = Vec::<String>::new(); sx_partial_clone.push(new_dcs_mtypes.clone()); 
       sx_partial_clone.push("start_offset:start_offset".to_string()); 
       sx_partial_clone.push("thread_i:thread_i".to_string());sx_partial_clone.push("save_dir:self.save_dir.clone()".to_string());
       sx_partial_clone.push("add_time:self.add_time".to_string()); sx_partial_clone.push("skip_m_type:self.skip_m_type".to_string());
       sx_partial_clone.push("buff_capacity:self.buff_capacity".to_string()); 
       sx_partial_clone.push("verbose:self.verbose".to_string()); 
    let mut sx_push_write:Vec<String> = Vec::<String>::new();
    let mut sx_flush:Vec<String> = Vec::<String>::new();
    for ix in 0..v_rbs.len() {
      little_element_names.push(format!("rb{}{}", v_rbs[ix].dc.to_lowercase(), v_rbs[ix].mtype.to_lowercase()));
      class_names.push(format!("ReadBuffer{}{}", v_rbs[ix].dc,v_rbs[ix].mtype));
      sx_el.push(format!("pub  {}:{}", little_element_names[ix], class_names[ix]));
      sx_let.push(format!("let mut {}:{} = {}::new(in_bufflen, thread_i, add_time, skip_m_type);", little_element_names[ix], class_names[ix], class_names[ix]));
      sx_new.push(format!("{}:{}", little_element_names[ix], little_element_names[ix])); 
      sx_clone.push(format!("{}:self.{}.clone()", little_element_names[ix], little_element_names[ix]));
      sx_partial_clone.push(format!("{}:self.{}.partial_clone(thread_i)", little_element_names[ix], little_element_names[ix]));
      sx_push_write.push(format!("rb{}{}.set_writer_clone(v_writer_locs[{}].give_mutex_to_a_thread());", v_rbs[ix].dc.to_lowercase(),v_rbs[ix].mtype.to_lowercase(),
        ix));
      sx_flush.push(format!("self.rb{}{}.flush_current_data();",v_rbs[ix].dc.to_lowercase(),v_rbs[ix].mtype.to_lowercase()));
    }
    sx_el.push("v_writer_locs:Vec<WriterLoc>".to_string());  sx_new.push("v_writer_locs:v_writer_locs".to_string());  
    sx_clone.push("v_writer_locs:self.v_writer_locs.clone()".to_string());
    sx_partial_clone.push("v_writer_locs:self.v_writer_locs.clone()".to_string());
    let complete_elements: TokenStream2 = TokenStream2::from_str( sx_el.join(",\n").as_str()).expect("Well Struct names should combine");
    let complete_let: TokenStream2 = TokenStream2::from_str( sx_let.join("\n").as_str()).expect("Complete Let should combine");
    let complete_new: TokenStream2 = TokenStream2::from_str( sx_new.join(",\n").as_str()).expect("complete_constructor fail");
    let complete_clone: TokenStream2 = TokenStream2::from_str( sx_clone.join(",\n").as_str()).expect("clone constructor");
    let complete_partial_clone: TokenStream2 = TokenStream2::from_str( sx_partial_clone.join(",\n").as_str()).expect("clone constructor");
    let complete_push_write: TokenStream2 = TokenStream2::from_str( sx_push_write.join("\n").as_str()).expect("push write thread");
    let complete_flush_all: TokenStream2 = TokenStream2::from_str( sx_flush.join("\n").as_str()).expect("flush a thread");
      
    //println!("Checking quality of Arc arrays.");
    //let a0 = Arc::new(arrow::array::Int64Array::from(vec![0 as i64; 10])) as ArrayRef;
    //let a1 = Arc::new(arrow::array::PrimitiveArray::<arrow::array::TimestampNanosecondArray>::from( &(vec![0 as i64,10])[0..4])) as ArrayRef;
    //let a_vec = vec![0 as i64;10];
    //let a1 = Arc::new(arrow::array::TimestampNanosecondArray::from( a_vec[0..4].to_vec())  ) as ArrayRef;
    //let a2 = Arc::new(arrow::array::Int64Array::from(a_vec[0..4].to_vec())) as ArrayRef;
    //let b_vec:Vec<u8> = vec![b'a',b'b',b'c',b'd'];
    //let b_try_vec:Vec<Vec<u8>> = b_vec.iter().take(3).map(|&b|vec![b]).collect();
    //let a3 = Arc::new(arrow::array::FixedSizeBinaryArray::try_from_iter(b_try_vec.iter()).unwrap()) as ArrayRef;

    /**************
    println!("Example: v_rbs[3] is {}", v_rbs[3]);
    println!("---- Here we print its code : \n{}", v_rbs[3].emit_read_buffer());
    println!("read_config_psv: v_rbs is length {}.", v_rbs.len());
    // Parsing Practice!
    let line  = " m, 10032, 432.3, asym,   30232,12.345,,, , ";
    println!(" Line is now --\"\"\"{}\"\"\"", line);
    let mut parts = line.split(',');
    let l0_c:char = parts.next().and_then(|s| Some(s.trim_start().chars().nth(0).unwrap_or('X'))).unwrap_or('X');
    let l1_i:i64 = parts.next().and_then(|s| if s.trim_start().len() == 0 { Some(NAI64) } else { s.trim_start().parse::<i64>().ok() }).unwrap_or_else( || {println!("Expected element 1 to be integer"); NAI64});
    let l2_i:f64 = parts.next().and_then(|s| if s.trim_start().len() == 0 { Some(NAF64) } else { s.trim_start().parse::<f64>().ok()}).unwrap_or_else( || {println!("Expected element 1 to be integer"); NAF64});
    let l3_sym = parts.next().and_then(|s|  Some(s.trim_start()) ).unwrap_or("");
    let l4_i:i64 = parts.next().and_then(|s| s.parse::<i64>().ok()).unwrap_or_else( || {println!("Expected element 1 to be integer"); -1});
    let l5_i:i64 = parts.next().and_then(|s|  if s.trim_start().len() == 0 { Some(NAD102) } else {Some({ 
                                           let mut ss = s.trim_start().split('.'); let x0 = 100 * ss.next().unwrap_or("0").parse::<i64>().unwrap_or(0 as i64);
                                           let mut cs1 = ss.next().unwrap_or("").chars();
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * 10 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64)
                                    })}).unwrap_or_else( || {println!("Expected to get l5d but did not."); NAD102});
    let l6_i:i64 = parts.next().and_then(|s| if s.trim_start().len() == 0 { Some(NAI64) } else { s.trim_start().parse::<i64>().ok() }).unwrap_or_else( || {println!("Expected element 1 to be integer"); NAI64});
    let l7_f:f64 = parts.next().and_then(|s| if s.len() == 0 { Some(NAF64) } else { s.parse::<f64>().ok()}).unwrap_or_else( || {println!("Expected element 1 to be integer"); NAF64});
    let l8_i:i64 = parts.next().and_then(|s|  if s.trim_start().len() == 0 { Some(NAD102) } else {Some({ 
                                           let mut ss = s.trim_start().split('.'); let x0 = 100 * ss.next().unwrap_or("0").parse::<i64>().unwrap_or(0 as i64);
                                           let mut cs1 = ss.next().unwrap_or("").chars();
                                           x0 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0) as i64) * 10 + (cs1.next().unwrap_or('0').to_digit(10).unwrap_or(0)  as i64)
                                    })}).unwrap_or_else( || {println!("Expected to get l5d but did not."); NAD102});

    let l9_i:i64 = parts.next().and_then(|s| if s.trim_start().len() == 0 { Some(NAI64) } else { s.parse::<i64>().ok() }).unwrap_or_else( || {println!("Expected element 1 to be integer"); NAI64});
    let l10_sym = parts.next().and_then(|s| Some(s.trim_start()) ).unwrap_or("");
    println!("read_config_psv: experimiental parse 0:{},1:{},2:{},3:\"{}\",4:{},5:{},6:{},7:{},8:{},9:{},10:\"{}\"", l0_c, l1_i, l2_i, l3_sym,l4_i,l5_i, l6_i, l7_f, l8_i, l9_i, l10_sym);
    ****/
    let mut v_buffer_tokens:Vec<TokenStream2>=Vec::<TokenStream2>::new();
    for ix in 0..v_rbs.len() {
      v_buffer_tokens.push(v_rbs[ix].emit_read_buffer());
    }
    //v_rbs[0].emit_read_buffer();
    //let ad = (records_data[0])[0].clone();
    let fake_writer = FakeWriter::new();
    let writer_loc_text = fake_writer.gen_writer_loc_text();
    //println!(" Before we generate code here is writer_loc_text");
    //println!("{}", writer_loc_text);
    let gen_code = quote! {
      #(#my_use_requirements)*

      #writer_loc_text
      
      #(#v_buffer_tokens)*

      pub struct CompleteReader {
        #complete_elements
      }
      impl CompleteReader {
        pub fn new(in_bufflen: usize, start_offset: u64, thread_i:i8, save_dir: String, 
          add_time:i64, skip_m_type:bool, buff_capacity:u64, verbose:i64) -> CompleteReader {
          #complete_let
          #qbmake
          #complete_push_write
          return CompleteReader{ 
            #complete_new
          }
        }
        #(#sx_parser_quotes)*
        #(#sx_complete_parse_by_line)*
        #(#sx_complete_parse_by_type)*
        #(#sx_all_bufreader_by_type)*
        pub fn finish(& mut self) {
          if self.verbose >= 1 {
            println!("CompleteReader -- Calling finisher for the complete reader.");
          }
          for ix in 0..self.v_writer_locs.len() { self.v_writer_locs[ix].finish(); }
        }
        pub fn flush_all(&mut self) {
          if self.verbose >= 2 {
            println!("Initiate Complete Reader flush all on thread = {}", self.thread_i);
          }
          #complete_flush_all
        }
        pub fn clone_update(&self, start_offset:u64, thread_i:i8) -> CompleteReader {
          return CompleteReader {
            #complete_partial_clone 
          }
        }
      }
      impl Drop for CompleteReader {
         fn drop(&mut self) {
           for ix in 0..self.v_writer_locs.len() {
             self.v_writer_locs[ix].finish();
           }
         }
      }
      impl Clone for CompleteReader {
        fn clone(&self) -> CompleteReader {
           return CompleteReader{
             #complete_clone 
           }
        }
      }
    };
   
     
    //println!("Compiling is so fun, here is the gen_code!");
    //println!("{}",gen_code);
    let gen_code:TokenStream = gen_code.into();
    //let generated_code = quote! {
      //#(records_data)\n*
      //let ad = (#records_data[0])[0].clone();
      //println!("Hey Something happened? First answer is {}?", 0)
      //#first_buffer
     // 0
      //println!("{}|{}|{}|{}",((#records_data)[ii])[0], (records_data[ii])[1],
      //                          ((#records_data[ii])[2],(records_data[ii])[3]));
    //};
     
     //"println!(
     gen_code.into()
    
     //return gen_code.into()
}
