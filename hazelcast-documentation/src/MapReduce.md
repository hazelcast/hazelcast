

## MapReduce

You have likely heard about MapReduce ever since Google released its [research white paper](http://research.google.com/archive/mapreduce.html) on this concept. With Hadoop as the most common and well known implementation, MapReduce gained a broad audience and made it into all kinds of business applications dominated by data warehouses.

MapReduce is a software framework for processing large amounts of data in a distributed way. Therefore, the processing is normally spread over several machines. The basic idea behind MapReduce is to map your source data into a collection of key-value pairs and reducing those pairs, grouped by key, in a second
step towards the final result.

The main idea can be summarized with the following steps.

  1. Read the source data.
  2. Map the data to one or multiple key-value pairs.
  3. Reduce all pairs with the same key.

**Use Cases**

The best known examples for MapReduce algorithms are text processing tools, such as counting the word frequency in large texts or websites. Apart from that, there are more interesting examples of use cases listed below.

 - Log Analysis
 - Data Querying
 - Aggregation and summing
 - Distributed Sort
 - ETL (Extract Transform Load)
 - Credit and Risk management
 - Fraud detection
 - and more...


