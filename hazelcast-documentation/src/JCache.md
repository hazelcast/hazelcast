 
# Hazelcast JCache Implementation

***NOTE:*** *This feature will be included in the next Hazelcast release.* 


Hazelcast provides JSR 107 implementation. In order to use Hazelcast as a JCache provider, just add `hazelcast-3.3-JCACHE.jar` and `cache-api` dependency.

## Setup

- Pull the latest from repo using the command `git pull origin jcache-preview` and use 
Maven install (or package) to build with `mvn clean install`.

- Add the file `hazelcast-3.3-JCACHE.jar` to your classpath.

- Download the `cache-api` from maven repo or add it as a dependency as shown below.

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Usage

After setting dependencies, you can start using JCache as described by the specification JSR 107.
Please note that Hazelcast specific configurations still can be performed as described by this document.

## Sample JCache Code

A sample code is shown below.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();

//configure the cache
MutableConfiguration<String, String> config = new MutableConfiguration<String, String>();
config.setStoreByValue(true)
.setTypes(String.class, String.class)
.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_MINUTE))
.setStatisticsEnabled(false);

//create the cache
cacheManager.createCache(name, config);

//get the cache
Cache<String, Integer> cache = cacheManager.getCache(name, String.class, String.class);
cache.put("theKey", "Hello World");
String value = cache.get("theKey");
System.out.println(value);//prints 'Hello World'
```

## Code Samples

Please see [Hazelcast Code Samples](https://github.com/hazelcast/hazelcast-code-samples) for more examples.


## Work In Progress

Below features are still in progress:

- Hazelcast Java client support for JCache
- Statistics and MXBean support 
- Synchronous listeners
- Near Cache extension
- Management Center integration
