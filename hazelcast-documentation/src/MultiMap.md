

## MultiMap

Hazelcast `MultiMap` is a specialized map where you can store multiple values under a single key. Just like any other distributed data structure implementation in Hazelcast, `MultiMap` is distributed and thread-safe.

Hazelcast `MultiMap` is not an implementation of `java.util.Map` due to the difference in method signatures. It supports most features of Hazelcast Map except for indexing, predicates and MapLoader/MapStore. Yet, like Hazelcast Map, entries are almost evenly distributed onto all cluster members. When a new member joins the cluster, the same ownership logic used in the distributed map applies.


### Sample MultiMap Code

Let's write code that puts data into a MultiMap.


```java
public class PutMember {
  public static void main( String[] args ) {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    MultiMap <String , String > map = hazelcastInstance.getMultiMap( "map" );

    map.put( "a", "1" );
    map.put( "a", "2" );
    map.put( "b", "3" ); 
    System.out.println( "PutMember:Done" );
  }
}
```

Now let's print the entries in this MultiMap.

```java
public class PrintMember {
  public static void main( String[] args ) { 
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    MultiMap <String, String > map = hazelcastInstance.getMultiMap( "map" );
    for ( String key : map.keySet() ){
      Collection <String > values = map.get( key );
      System.out.println( "%s -> %s\n",key, values );
    }
  }
}
```

After you run the first code sample, run the `PrintMember` sample. You will see the key **`a`** has two values, as shown below.

`b -> [3]`

`a -> [2, 1]`

### MultiMap Configuration

When using MultiMap, the collection type of the values can be either **Set** or **List**. You configure the collection type with the `valueCollectionType` parameter. If you choose `Set`, duplicate and null values are not allowed in your collection and ordering is irrelevant. If you choose `List`, ordering is relevant and your collection can include duplicate and null values.

You can also enable statistics for your MultiMap with the `statisticsEnabled` parameter. If you enable `statisticsEnabled`, statistics can be retrieved with `getLocalMultiMapStats()` method.
<br></br>

***RELATED INFORMATION***

*Please refer to [MultiMapConfig.java](https://github.com/hazelcast/hazelcast/blob/b20df7b1677e00431ceddb7e90a0e3615a3e9914/hazelcast/src/main/java/com/hazelcast/config/MultiMapConfig.java) for more information on configuration options.*


