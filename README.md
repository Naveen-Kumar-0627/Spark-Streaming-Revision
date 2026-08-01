# Basics of Spark streaming
## What is Strucutred Streaming
- Normally  what we data is stored somewhere we process it ,done.
- Streaming - data is keep coming little bit ,little bit over time.we keep process it as it come.
- Normal Spark - when new files added we need process it manually
- Streaming- when new files arrives spark detects it itself and process it automatically.
  ## simple example
  `# ReadStream
  df=spark.readStream.format('delta').load('path')
  # WriteStream
  df.WriteStream.format('delta')\
  .option('checkpointlocation','path')\
  .table('path')
  `
### Stateless Transformation 
- stateless transformations are narrow transformation.
- each batch processed independently ,does not need to remember anything from previous batches.
- basically select,filter,union,explode ...
### StateFull Transformation
- statefull transforamtions are wide transactions.
- -each 
  
