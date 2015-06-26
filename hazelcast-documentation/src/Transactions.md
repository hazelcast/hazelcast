

# Transactions

You can use Hazelcast in transactional context. 

## Transaction Interface

You create a `TransactionContext` to begin, commit, and rollback a transaction. You can obtain transaction-aware instances of queues, maps, sets, lists, multimaps via `TransactionContext`, work with them, and commit/rollback in one shot.

Hazelcast supports two types of transactions: LOCAL (One Phase) and TWO\_PHASE. With the type, you have influence over how much guarantee you get when a member crashes while a transaction is committing. The default behavior is TWO\_PHASE.

- **LOCAL**: Unlike the name suggests, LOCAL is a two phase commit. First, all cohorts are asked to prepare; if everyone agrees, then all cohorts are asked to commit. A problem can happen during the commit phase: if one or more members crash, then the system could be left in an inconsistent state.

- **TWO\_PHASE**: The TWO\_PHASE commit is more than the classic two phase commit (if you want a regular two phase commit, use local). Before TWO\_PHASE commits, it copies the commit log to other members, so in case of member failure, another member can complete the commit.

```java
import java.util.Queue;
import java.util.Map;
import java.util.Set;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Transaction; 

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

TransactionOptions options = new TransactionOptions()
    .setTransactionType( TransactionType.LOCAL );
    
TransactionContext context = hazelcastInstance.newTransactionContext( options )
context.beginTransaction();

TransactionalQueue queue = context.getQueue( "myqueue" );
TransactionalMap map = context.getMap( "mymap" );
TransactionalSet set = context.getSet( "myset" );

try {
  Object obj = queue.poll();
  //process obj
  map.put( "1", "value1" );
  set.add( "value" );
  //do other things..
  context.commitTransaction();
} catch ( Throwable t ) {
  context.rollbackTransaction();
}
```

In a transaction, operations will not be executed immediately. Their changes will be local to the `TransactionContext` until committed. However, they will ensure the changes via locks. 

For the above example, when `map.put` is executed, no data will be put to the map but the key will get locked for changes. While committing, operations will be executed, the value will be put to the map, and the key will be unlocked.

Isolation level in Hazelcast Transactions is `READ_COMMITTED`. If you are in a transaction, you can read the data in your transaction and the data that is already committed. If you are not in a transaction, you can only read the committed data.

![image](images/NoteSmall.jpg) ***NOTE:*** *The REPEATABLE_READ isolation level can also be exercised using the method `getForUpdate()` of `TransactionalMap`.*

Implementation is different for queue/set/list and map/multimap. For queue operations (offer, poll), offered and/or polled objects are copied to the owner member in order to safely commit/rollback. For map/multimap, Hazelcast first acquires the locks for the write operations (put, remove) and holds the differences (what is added/removed/updated) locally for each transaction. When the transaction is set to commit, Hazelcast will release the locks and apply the differences. When rolling back, Hazelcast will release the locks and discard the differences.

`MapStore` and `QueueStore` does not participate in transactions. Hazelcast will suppress exceptions thrown by store in a transaction. Please refer to the [XA Transactions section](#xa-transactions) for further information.

### LOCAL vs. TWO_PHASE

As it can be understood from the above [Transaction Interface section](#transaction-interface), when you choose LOCAL as the transaction type, Hazelcast tracks all changes you make locally in a commit log, i.e. list of changes. In this case, all the other members are asked to agree that the commit can succeed and once it is agreed, Hazelcast starts to write the changes. 
However, if the member that initiates the commit crashes after it has written to at least one member (but has not completed writing to all other members), your system may be left in an inconsistent state.

On the other hand, if you choose TWO_PHASE as the transaction type, the commit log is again tracked locally but it is copied to another cluster member. Therefore, when a failure happens (e.g. the member initiating the commit crashes), you still have the commit log in another member and that member can complete the commit. However, copying the commit log to another member makes the TWO_PHASE approach slow.

Consequently, it is recommended that you choose LOCAL as the transaction type if you want a better performance and TWO_PHASE if reliability of your system is more important than the performance. 

