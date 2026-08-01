## what is state 
-state stores previous batches computed result for upcoming batches computations
### why its matters
   -each batch has its part of data only but aggregations ,group by needs previos results ,so with this its compute 
   result stores as new state
### where its stored
 - RocksDB :In executors,until the batch processed
 - when computation finished then state data copied into Checkpoint Location
## state less transformation
  - state less transformations are narrow transformation ,its not needed state memory
  - example:
    - select,filter operations
## statefull transformation
   -state full transformations are wide transaformation,its uses state memory
   -example :
     - groupby,aggregations
##window
-groups data into based on time period ,process the data when data falls within the  time period and computes ,stores it into state
-example
`.groupBy(window(col('time_stamp_col'),'10 minues')`
##watermark 
- to clean the state memory,telling spark to how long you wait for late arriving data
- -example
- `.withwaterMark('time_stamp_col','10 minutes')`
