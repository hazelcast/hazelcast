
# Hazelcast MapReduce

Ever since Google released its [research white paper on map-reduce](http://labs.google.com/papers/mapreduce.html)
you've heard about it. With hadoop as the most common and well known implementation, map-reduce received a broad
audience and made it into all kinds of business applications dominated by data-warehouses.

From what we see at the white paper map-reduce is a software framework for processing large amounts of data in a
distributed way. Therefor the processing normally is spread over several machines. The basic idea behind map-reduce
is to map your source data into a collection of key-value pairs and reducing those pairs, grouped by key, in a second
step towards the final result.

The main idea can be written down using 3 simple steps:

  1. Read source data
  2. Map data to one or multiple key-value pairs
  3. Reduce all pairs with the same key

**Use Cases**

The best known example for map-reduce algorithms are text processing like counting the word frequency in large
texts or websites but there are more interesting example use cases out there like:

 - Log Analysis
 - Data Querying
 - Aggregation and summing
 - Distributed Sort
 - ETL (Extract Transform Load)
 - Credit and Risk management
 - Fraud detection
 - and more...

