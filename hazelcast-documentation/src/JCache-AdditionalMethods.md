
### Additional Methods

In addition to the operations explained in the [Async Operations section](#async-operations) and [Custom ExpiryPolicy section](#custom-expirypolicy), Hazelcast ICache also provides a set of convenience methods. These methods are not part of the JCache specification.

 - `size()`: Returns the estimated size of the distributed cache.
 - `destroy()`: Destroys the cache and removes the data from memory. This is different from the method `javax.cache.Cache::close`.
 - `getLocalCacheStatistics()`: Returns a `com.hazelcast.cache.CacheStatistics` instance providing the same statistics data as the JMX beans. This method is not available yet on Hazelcast clients: the exception `java.lang.UnsupportedOperationException` is thrown when you use this method on a Hazelcast client.

