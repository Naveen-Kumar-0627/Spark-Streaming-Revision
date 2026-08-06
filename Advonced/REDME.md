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