# Basics of Spark streaming
## What is Strucutred Streaming
- Normally  what we do data is stored somewhere we process it ,done.
- Streaming - data is keep coming little bit ,little bit over time.we keep process it as it come.
- example
   - imagine a folder ,every few seconds a new file is added to this folder
   - Normal Spark - when new files added we need process it manually
   - Streaming- when new files arrives spark detects it itself and process it automatically.
  ## simple example
  ```
  #ReadStream
  df=spark.readStream\
  .format('delta')\
  .load('path')
  #WriteStream
  df.WriteStream.format('delta')\
  .option('checkpointlocation','path')\
  .table('path')
  ```
## CheckPointLocation
`.option('checkpoinLocation','path')` 
### why we need this
  -imagine spark read 5 rows > processed it > write to output table
  -in the checkpoint folder spark write note like i read upto 5 rows
  - when stream job crashes , re-running the job ,spark checks checkpoint location, things like 'oh read upto 5 rows now i       need move on  from 6th row'
### without this  
  - spark re -reads whole data and write it as output means duplicates the data
## Triggers
  - Telling spark to check for new data often this time
  - ```
    .trigger(processingTime= "1 minute")
     ```
     - 1 minute means telling spark to get up after 1 minute and check up is any new data arrived
    .`trigger(availableNow=True) ` means checks only once (when manually running the code)


 ## Output Modes 
 ### 1.append 
    - just new rows are added ,existing rows not touched ,not changed
    -example
       - id ,name
         1  ,a
         2  ,b
         new row 2,c
         1,a
         2,b
         2,c
### 2.         
 
