# what is state 
-state stores previous batches computed result for upcoming batches computations
## why its matters
   -each batch has its part of data only but aggregations ,group by needs previos results ,so with this its compute 
   result stores as new state
## where its stored
 - RocksDB :In executors,until the batch processed
 - when computation finished then state data copied into Checkpoint Location
# state less transformation
  - state less transformations are narrow transformation ,its not needed state memory
  - example:
    - select,filter operations
# statefull transformation
   - state full transformations are wide transaformation,its uses state memory
   - example :
     - groupby,aggregations
# window
- groups data into based on time period ,process the data when data falls within the  time period and computes ,stores it into state
- example
`.groupBy(window(col('time_stamp_col'),'10 minues')`
# watermark 
- to clean the state memory,telling spark to how long you wait for late arriving data
  -example
  - `.withwaterMark('time_stamp_col','10 minutes')`
  - formula: max(event_time) - 10 minutes ,so per given 10 minutes older state will be deleted
# Why we use manual over autoSchema 
  ## To avoid spark to Scan the same batch for Twice
    - first time reads it for the infer the schema
    - second time reads it for write
  ## To avoid downstream failuer
    ### with autoSchema
    - batch1 amount column is int type
    - but batch2 amount column is float,so downstream crashes will happen
    ### with manual schema 
    - if column type mismatched stream will be failed 
    - no crashe for good quality there in downstream system 
  ## Supports fault tolarance and recover
     - if stream fails due to schema,spark can re-run from where its stopped using checkpoint,metadata,manual schema  
      
  ## Lack computation power for down transformation
    - spark is eager to scan  each batch for render the schema so its lot computation power
    - so down side transformation will not get computation power
# Window + Aggregation + Watermark 
    - here Window stores one aggregated row per window,for each window group
    - if window period is ended spark finals the window,but using Watermark period spark can still re-open that window for late arrive   records     
    - if water mark period is ended for late arrived record then that row will be dropped silently
# idempotency
   - Once data is processed spark will not process same data event the file name changes.
   - spark achives idempotency using or referring the Check Point Location
# Clean Source 
   - when streaming reads from directory like reading json,csv. ` clean source ` controls  what do next after reading the source file
   ## Modes 
    ### default:
      - source files are untouched ,metadata keeps eye on what it reads.so only new data processed .eventually source file count grows its slows down listing
    ### delete :
     - once file is processed spark deletes the source file from directory  after commiting.
    ### archive:
     - once file is processed and committed,spark moves source file from source directory into other directory 
     ### example code:
      ``` spark.readStream.format('json')\
          .option('cleanSource','archive')\
          .option('sourceArchiveDir','path')\
          .load('path/to/source')  ```
# foreachBatch
  - foreach batch is a sink ,this allow us to perform batch operation on streaming query ,so we can perform certain operations on each-batch output as like dataframe
  - operations : merge,writing to multiple sinks...
  ## Promblem it solves 
   ### without foreachBatch
   - for example if we need to write same streaming output of each batch to multiple sinks ,we do like this.
     ```df.writeStream\
         .format('json')\
         .option('checkpointLocation','path1')\
         ... ```

      ```df.writeStream\
         .format('jdbc')\
         .option('checkpointLocation','path2')\
         ... ```
    - so spark need to maintain `2 checkpoint location` for write into 2 sinks
    - here 2 writes so its need to read same data for twice
  ### With foreacBatch 
      ```def writer(output_df,batch_id):
             output_df.write('json')..
             output_df.write('jdbc')..
        df.writeStream\
          outputMode('append')\
          .foreachBatch(writer)\
          .trigger(processingTime="10 seconds")\
          .option("checkpointLocation',"path")\
          .start() ```
    - one checkpointlocation 
    - one time read ,not depend on how many sink to write    
    - we can also perform merge operations   
        
# Types of Window 
  ## Tumbling
     - fixed time,no overlapping,every event belongs to only one event 
     - ex: ```df.writeStream\
             .groupBy(window("event_time","10 minutes")) ```
        ex: events  with event time 12:00 to 12:10 belongs to  one bucket.
  ## Sliding window
      - window size is fixed time but it can overlap with other windows,one event can fall on  multiple windows 
      ex : ``` df.writeStream\
                .groupBy(window("event_time","10 minutes","5 minutes"))\ ```    
          ex:events will fall on 2 windows  
  ## Session window 
    - session window  is now fixed window,but it have gap time to close the window 
    - if no data arrives within the gap period then the window will be closed,if data arrived session window will be extended like `event_time + gap_time`
    ex:
      ``` df.writeStream\
             .groupBy(session_window("event_time","5 minutes"))   ```

    ex:event with event time 12:00,spark wait `event_time + 5 min (12:00 + 5 =12:05)`,if no records arrive within  this time window will be closed ,or another event arrive at 12:4 so`(12:4 + 5min =12:9)` spark wait untill this time for another event
        

