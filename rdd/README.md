## RDD creation and partitions
### Question 1
The number of partitions created by textFile() depends on the size of the file and Hadoop's input split configuration. Since the files used in this lab are relatively small, Spark created only 2 partitions.

### Question 2
repartition() always performs a shuffle to redistribute the data between partitions.

coalesce() can reduce the number of partitions without performing a full shuffle, making it more efficient when decreasing the partition count.


## Lazy evaluation and lineage

### Question 1

Yes, calling orders_data.count() a second time creates a new job because Spark recomputes the lineage each time an action is executed.

### Question 2

Using .cache() before the first action would change this behavior. After the first computation, Spark can reuse the cached partitions instead of recomputing the entire lineage.



## The cost of no schema: the multiline address

### Question 1

spark.read.csv(multiLine=True) understands the CSV structure and can correctly read records containing line breaks inside fields. `textFile()` only reads physical lines and therefore splits records incorrectly.

### Question 2

For a line-oriented reader such as textFile(), each logical record must be stored on a single physical line. Otherwise records can be broken into multiple elements.

## Manual parsing of orders

### Question 1

If a product contained an extra comma, split(",") would return more values than expected and the parsing would fail when Spark executes the transformation.

### Question 2

The DataFrame API performs parsing and schema validation while reading the file and creating the DataFrame.

## Key-value RDDs: total quantity per user

### Question 1

reduceByKey() performs partial aggregation on each partition before the shuffle. groupByKey() shuffles all values associated with a key, which is more expensive because more data must be transferred across partitions.

### Question 2

groupByKey() is necessary when all values must remain available together, for example when computing a median or building a complete list of values.

## Manual deduplication

### Question 1

The shuffle performed by reduceByKey() plays the same role as the repartitioning step in the window function. It brings all records with the same order_id to the same partition.

### Question 2

The window function expresses the intent more clearly because it directly describes keeping the most recent record for each order_id.

## Joining orders with users

### Question 1

Spark could avoid the shuffle if both RDDs used the same partitioner, the same number of partitions, and were partitioned on user_id.

### Question 2

With leftOuterJoin(), all orders are kept. If an order references a user that does not exist, the user information is returned as None.

### Question 3

Calling collect() on a large dataset can overload the driver's memory because all data is transferred from the executors to
the driver process.
