

### Map Locks

Hazelcast Distributed Map is thread-safe and meets your thread safety requirements. When these requirements increase or you want to have more control on the concurrency, below features and solutions provided by Hazelcast can be considered.

Let's work on a sample case as shown below.

```java
public class RacyUpdateMember {
    public static void main( String[] args ) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Value> map = hz.getMap( "map" );
        String key = "1";
        map.put( key, new Value() );
        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            if ( k % 100 == 0 ) System.out.println( "At: " + k );
            Value value = map.get( key );
            Thread.sleep( 10 );
            value.amount++;
            map.put( key, value );
        }
        System.out.println( "Finished! Result = " + map.get(key).amount );
    }

    static class Value implements Serializable {
        public int amount;
    }
}
```

If the above code is run by more than one cluster members simultaneously, there will be likely a race condition.

#### Pessimistic Locking

One usual way to solve this race issue is using the lock mechanism provided by Hazelcast distributed map, i.e. `map.lock` and `map.unlock` methods. You simply lock the entry until you finished with it. See the below sample code.

```java
public class PessimisticUpdateMember {
    public static void main( String[] args ) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Value> map = hz.getMap( "map" );
        String key = "1";
        map.put( key, new Value() );
        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            map.lock( key );
            try {
                Value value = map.get( key );
                Thread.sleep( 10 );
                value.amount++;
                map.put( key, value );
            } finally {
                map.unlock( key );
            }
        }
        System.out.println( "Finished! Result = " + map.get( key ).amount );
    }

    static class Value implements Serializable {
        public int amount;
    }
}
```

The IMap lock will automatically be collected by the garbage collector when the map entry is removed.

The IMap lock is reentrant, but it doesn't support fairness.

Another way can be acquiring a predictable `Lock` object from Hazelcast. By this way, every value in the map can be given a lock or you can create a stripe of locks.


#### Optimistic Locking

Hazelcast way of optimistic locking is to use `map.replace` method. See the below sample code. 

```java
public class OptimisticMember {
    public static void main( String[] args ) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Value> map = hz.getMap( "map" );
        String key = "1";
        map.put( key, new Value() );
        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            if ( k % 10 == 0 ) System.out.println( "At: " + k );
            for (; ; ) {
                Value oldValue = map.get( key );
                Value newValue = new Value( oldValue );
                Thread.sleep( 10 );
                newValue.amount++;
                if ( map.replace( key, oldValue, newValue ) )
                    break;
            }
        }
        System.out.println( "Finished! Result = " + map.get( key ).amount );
    }

    static class Value implements Serializable {
        public int amount;

        public Value() {
        }

        public Value( Value that ) {
            this.amount = that.amount;
        }

        public boolean equals( Object o ) {
            if ( o == this ) return true;
            if ( !( o instanceof Value ) ) return false;
            Value that = ( Value ) o;
            return that.amount == this.amount;
        }
    }
}
```

***NOTE:*** *Above sample code is intentionally broken.*

#### Pessimistic vs. Optimistic Locking

Depending on the locking requirements, one locking strategy can be picked.

Optimistic locking is better for mostly read only systems and it brings a performance boost over pessimistic locking.

Pessimistic locking is good if there are lots of updates on the same key and it is more robust than optimistic one from the perspective of data consistency.
In Hazelcast, use `IExecutorService` for submitting a task to a key owner or to a member, or members. This is the recommended way of task executions which uses pessimistic or optimistic locking techniques. By following this manner, there will be less network hops and less data over wire and also tasks will be executed very near to data. Please refer to [Data Affinity](#data-affinity).

#### ABA Problem

ABA problem occurs in environments when a shared resource is open to change by multiple threads. So, even one thread sees the same value for a particular key in consecutive reads, it does not mean nothing has changed between the reads. Because one another thread may come and change the value, do another work and change the value back, but the first thread can think that nothing has changed.

To prevent these kind of problems, one possible solution is to use a version number and to check it before any write to be sure that nothing has changed between consecutive reads. Although all the other fields will be equal, the version field will prevent objects from being seen as equal. This is called the optimistic locking strategy and it is used in environments which do not expect intensive concurrent changes on a specific key.

In Hazelcast, you can apply optimistic locking strategy by using `replace` method of map. This method compares values in object or data forms depending on the in memory format configuration. If the values are equal, it replaces the old value with the new one. If you want to use your defined `equals` method, in memory format should be `Object`. Otherwise, Hazelcast serializes objects to binary forms and compares them.  