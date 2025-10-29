# SRC files for generating/reading multi line format data
This simiplified format should be mostly self explanatory.  

We need to know:
  1. Is this a D or C based input file (aka a type of feed, such as "Direct or Consolidated Feed" in HFT data)
  2. What is the Message type (single character message types are ideal, though we have tested to have variable length
     strings or fixed length multi-char inputs depending on if data providers change their message markers.
     The message markers could be 1st or 2nd characters in a target CSV but here we start with it being 1st character.
  3. What is the column name
  4. What is the column type for loading (examples i64,i32,D185:Decimal(18,5),"c2":ASCI-Char[2], TNS a nanosecond eligble integer...
  5. Where do you want this column in saved parquet (negative numbers for columns we will drop.

     
