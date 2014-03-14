
# Hazelcast MapReduce


You have heard about MapReduce ever since Google released its [research white paper](http://research.google.com/archive/mapreduce.html) on this concept. With Hadoop as the most common and well known implementation, MapReduce gained a broad audience and made it into all kinds of business applications dominated by data warehouses.

From what we see at the white paper, MapReduce is a software framework for processing large amounts of data in a distributed way. Therefore, the processing is normally spread over several machines. The basic idea behind MapReduce is to map your source data into a collection of key-value pairs and reducing those pairs, grouped by key, in a second
step towards the final result.

The main idea can be summarized with below 3 simple steps.

  1. Read source data
  2. Map data to one or multiple key-value pairs
  3. Reduce all pairs with the same key

**Use Cases**

The best known examples for MapReduce algorithms are text processing tools like counting the word frequency in large texts or websites. Apart from that, there are more interesting example use cases as listed below.

 - Log Analysis
 - Data Querying
 - Aggregation and summing
 - Distributed Sort
 - ETL (Extract Transform Load)
 - Credit and Risk management
 - Fraud detection
 - and more...
