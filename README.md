# Demo of a Parquet-based Arrow File breaker

 -- Alan Lenarcic 2025

 -- GNU Public License: this is open source, but you should probably write your own

## Framework for building customized readers for multi-format csv/psvs.

  Some data feeds combine multiple feeds together and produce a single file with
records of different schemas mixed together. Documenting several-layer schema issues 
can be difficult and lead to ad-hoc and hard-to-replicate code.  Here we allow
a user to specify the variety of schema, in a table-oriented "c0.psv" format configuration
file.  Rust Procedural macros, at compile time, read the configuration files,
and generate suitable reader structures to power a an extract-from-csv-to-parquet operation.
As needed, this code can be adjusted to make subtle adjustments, that may be difficult to 
achieve in chaining fixed utilities together with scripts or python.

The optimal scheme for extracting CSVs can be system dependent.
Some Cloud/On-prem instances can handle files with access to sizable system ram/and 
support heavy multithreading, other users may face a default with significantly 
less performance, memory, disk-access, and local threads.
A typical use-case we research is a typically nightly size 2B line files.
One goal of this is to achieve compression on server instances, 
in the form of a columnar parquet table, which can reduce data size and prepare
the file to be processed into a HIVE format (where additional sorting, record 
reconcilation, etc may be performed).  While it may take an hour to open CSVs,
 the resulting parquet files can typically be read in seconds, and then-after be
 resorted to and preprocessed to achieve even-more tuned datastructures on disk.
 
  We do observe that, through bypassing the many warning flags, DuckDB can be
 used on this file and achieve mostly adequate performance in speed and compression.

  This package is designed to investigate and emonstrate the feasibility of a Rust-
based solution. We test whether Rust gives adequate tools, such that, 
using pre-compliationand so-called "procedural macros", it can built data structures and read a 
sequenced files, depositing the result in compressed parquet-style files, using
every core on the platform.  
  This can be challenging since C++ based programms
like DuckDb already implement very performant technology for CSV datastructures.
We observe that duckdb can be forced (with much effort) to deal with these
differentially suplied CSV files and deliver rather effiicient CSV readers,
despite every warning flags' insistence to the contrary.
  That said, in future extractions, it may be that a universal, pre-compiled,
infrastructure, the line-by-line reader might be essential.  To support this,
this library example demonstrates Rust-based competitive code that can be
manually editted at the line-level to compete against pre-compiled methods.
  Over time, alternate compression formats such as OpenZL could be used, or some
 of the data integrity checks (for instance, sort by "order id" and identify all
 unclosed orders) might be possible at the data-reader layer (though this can
 ofen be done efficiently if the data is already ingested and compressed in parquet.
 
  We inevitably find that a low-power laptop can ingest approximately 100M lines in 230 seconds
 which would be acceptable for a 2B line file to be completed in 1-2 Hours.  This would have
to deal with known issues affecting data/hard-drive speeds on cloud platforms.

# The lib-ptc input and output files.
   The problem with multiple programming languages/data-structures/
 operating-systems/packages is that the types of every column of data
 in a CSV may be described by a variety of particular names.  
 It appears that any user of Rust/C++/C/Parquet/OpenZL/DuckDB has to master a limitless
 variety of types and translate these concepts between the projects

  See "Int32...'i'...'32'...i32...Integer32...Int4...Integer...Short": integer types are
 invariable described in multiple types depending on the language and library.

  Here we look of a single standard file (Example "test_src/c0_0.psv")
 which shows how we might describe the variety of contexts a single
 variable type csv would show columns based upon data in the first column.

  Of course, a provider might not encode types in a single character
  (variety types "T", "t", "FT", "trade", "Trade", "Fill" might
  all be particucularly assigned by a data provider for messages of a single
  trade type)

  This is the purpose of a descriptor "ptc/src" file where the multiple
 potential types of each column are encoded.  We describe:
 0. The "Direct or Consolidated" (d/c) feed where the data comes from.
 1. The Message Type (2/T/M/D/C/U/...) "message type" (mtype)
   which would be the first (or maybe second)  entry in each 
   line of data, which alters column format....
 2. Depending on column type, but most likely first is a Timestamp
   microseconds after midnight.
 3.  ... likely the delta for a recorder.
 4.  ... Keep going


  We see that a programming language, understanding Rust semantics,
 may be able to take this kind of input an design a file reader.

  That said, Rust semantics are remarkably brittle, so pre-compiling
 to protect all sorts of types and getting them ready to add to 
 Parquet/Arrow/Arrow_IPC compatible record batches is challenging.

  Here we achieve that, for better or worse, using a procedural-macro
 approach that uses pre-developed input files to help construct
 a reader framework that can work line-by-line on an input file.

## Supported type examples  :
   We expect a single file which we can use to build a model of data, the
  "ptc/save_data" files.   Users can be trained to design these files for
a number of the pre-defined column formats.  Basics can be inferred:
 - i64: integer 64 bit type, u64 likewise
 - u32 ... likewise based upon Rust common types
 - f32 ... continuing
 - d102 ... Working on some challenges,  a Decimal format 
    of 10 places of precision and 2 decmials (like Dollars $1.02)
 - d1805 .. a basic exmaple of a 18 and 5 decimal 
    (further can be implemented simply)
 - c1 a single ascii character (Rust is "char=U64" default based")
 - c2 for a two asci character &[2;u8] type (see above)
 - TNS/TUS/DLt timestamp pre-configured types, dealing with data providers
   who may count a different amount of discrete ticks past midnight/default
   for thier timestamps
 - STR/Str/str: String types, stored intiiatlly as &str- big array, until
  maybe sometimes later turned into ENUMs

##  A demonstration of "Procedural Macros"
  Rust offers two types of pre-compilation "Macro" operators that can
 write code before final compilation.  The "Declartive macros" are 
 more easy to use, with common types "println!" and "macro!" declared
 by the "!" operator, and often written with the "macro_rules!" operator.

 Meanwhile "procedural macros" can be used to write an entire library of 
 code before compiling a main part of the package.  As suce, the "procedure" exists
 in an entirely separate package.  In this case "rcsv/lib_rcsv/src/lib.rs" is a
 standalone Rust library that is only compiled in conjunction to a target
 library, like the example in our rcsv/src/main.rs file.

 The procedural macros largely compile into "TokenStream" records.
 Note that even "TokenStream" is a volatile Rust format.  In reality,
 procedural-macro programmers will likely use a "quote!" macro and an
 entirely differenent "proc_macro2::TokenStream" -- (aka in this package
  "TokenStream2") macros, that can be converted with difficulty into a final
 "TokenStream" based output.  Pure "Tokenstream" can be introduced by the
 RUST compiler, and then compiled as in-line code to determine the final
 compiled executable.

 Given the fragile nature of the lib.rs code, treat this as a first-time
 amateur effort to use Rust in a typical CSV reading situation.  This code
 demonstrates the major extraction decisions that need to be made including:
  1. Creating separate structures for each table type that can be cloned
      for multiple threads.  
  2. Creating Mutex locked Writer Files (either Arrow_IPC or Parquet so far)
    where the threads wait for single access and then flush results at
    a user defined flush rate. (should depend on available RAM on computer and
    will effect the RecordBatch size on disk)
  3. Two solutions to reading a file
    a. Read file 4 times for 4 file types, masking out irrelevant lines
    b. Read file line by line to 4 different eligible readers.
  4. Constructing Arrow Recordbatch out of Rust vector buffers.
    a. Loading Strings as either ```Vec<String>``` (slightly slower than 
      a single "String" with "|" separators converted to vec<&str>
    b. How to create u8 buffer arrays for fixed size 1 and 2 Ascii character entries
    c. How to parse "decimals" of a user supplied size and precision where they are
      stored as integer starting at goal decimal point.  Here we load as rust Vec<int>
      and cast into correct decimal when passing to arrow
    d. Float/Int types seem pretty self explanatory.
  5. Compression ratios for Arrow or Parquet choices.  Note that ZSTD compression
   to parquet can be 10 times slower than Arrow_IPC method, but that Arrow IPC seem
   to be about twice the size of Parquet.  Further experimentation with OpenZL
   format may be possible if there are more programs compatible and we can establish
   ability to write to OpenZL in batches.

  





