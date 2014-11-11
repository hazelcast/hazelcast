

## IdGenerator

Hazelcast IdGenerator is used to generate cluster-wide unique identifiers. Generated identifiers are long type primitive values between 0 and `Long.MAX_VALUE`. 

ID generation occurs almost at the speed of `AtomicLong.incrementAndGet()`. A group of 1 million identifiers is allocated for each cluster member. In the background, this allocation takes place with an `IAtomicLong` incremented by 1 million. Once a cluster member generates IDs (allocation is done), `IdGenerator` increments a local counter. If a cluster member uses all IDs in the group, it will get another 1 million IDs. By this way, only one time of network traffic is needed, meaning that 999,999 identifiers are generated in memory instead of over the network. This is fast.

Let's write a sample identifier generator.

```java
public class IdGeneratorExample {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    IdGenerator idGen = hazelcastInstance.getIdGenerator( "newId" );
    while (true) {
      Long id = idGen.newId();
      System.err.println( "Id: " + id );
      Thread.sleep( 1000 );
    }
  }
}
```

Let's run the above code two times. The output will be similar to the following.

```plain
Members [1] {
  Member [127.0.0.1]:5701 this
}
Id: 1
Id: 2
Id: 3
```


```plain
Members [2] {
  Member [127.0.0.1]:5701
  Member [127.0.0.1]:5702 this
}
Id: 1000001
Id: 1000002
Id: 1000003
```

You can see that the generated IDs are unique and counting upwards. If you see duplicated identifiers, it means your instances could not form a cluster. 


![image](images/NoteSmall.jpg) ***NOTE:*** *Generated IDs are unique during the life cycle of the cluster. If the entire cluster is restarted, IDs start from 0 again or you can initialize to a value using the `init()` method of IdGenerator.*

![image](images/NoteSmall.jpg) ***NOTE:*** *IdGenerator has 1 synchronous backup and no asynchronous backups. Its backup count is not configurable.*


