# Concurrent HD data structure

|ℹ️ Since: 4.1| 
|-------------|

## Background

HD data structures are not thread-safe and can be accessed only from the partition thread to guarantee sequential
access. The IMap's indexing data structure is partitioned to guarantee the access is thread-safe. For the new SQL
engine, this limitation is crucial, and we have to access the HD data structures from a query thread without 
offloading operations to the partition thread. 

This document describes a new concurrent HD data structure based on the well-known B+tree data structure to provide
for the thread-safe access to HD data. This data structure is used as a backbone for global concurrent HD index 
that is used by the SQL engine to run both lookup and full scan operations. 

## User Interaction

### API design 

There are no public API changes since the data structure is used as a backbone for other data structures like 
indexes. However, `BPlusTree` is an entry point interface to the concurrent B+tree implementation. The `lookup`
method returns an iterator over results and doesn't materialize the full result set, thus, avoiding `OOME`
for big data set. 

```java
/**
 * Represents an API for off-heap concurrent B+Tree index.
 * @param <T> the type of the lookup/range scan entries
 */
public interface BPlusTree<T extends QueryableEntry> extends Disposable {
    /**
     * Inserts new entry into B+tree. Overwrites old value if it exists.
     *
     * @param indexKey the index key component
     * @param entryKey the key of the entry corresponding to the indexKey
     * @param value    a reference to the value to be indexed
     * @return old value if it exists, {@code null} otherwise
     */
    NativeMemoryData insert(Comparable indexKey, NativeMemoryData entryKey, MemoryBlock value);
    /**
     * Removes an entry from the B+tree.
     *
     * @param indexKey the index key component
     * @param entryKey the key of the entry corresponding to the indexKey
     * @return the old removed value if it exists, {@code null} otherwise
     */
    NativeMemoryData remove(Comparable indexKey, NativeMemoryData entryKey);
    /**
     * Looks up the index and returns an iterator of entries matching the indexKey.
     *
     * @param indexKey the index key to be searched in the index
     * @return the iterator of {@code QueryableEntry} matching the lookup criteria
     * @throws IllegalArgumentException if the indexKey is {@code null}
     */
    Iterator<T> lookup(Comparable indexKey);
    /**
     * Returns a range scan iterator of entries matching the boundaries of the range.
     *
     * @param from          the beginning of the range
     * @param fromInclusive {@code true} if the beginning of the range is inclusive,
     *                      {@code false} otherwise.
     * @param to            the end of the range
     * @param toInclusive   {@code true} if the end of the range is inclusive,
     *                      {@code false} otherwise.
     * @return the iterator of entries in the range
     */
    Iterator<T> lookup(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive);
    /**
     * Returns an iterator of all unique index keys in the B+tree.
     * @return the iterator of all unique index keys
     */
    Iterator<Data> keys();
    /**
     * Removes all entries from the B+tree.
     */
    void clear();
}
```

### Old Query and SQL engines
The main design goal for the new concurrent data structure is to use it for the SQL engine. Without it the SQL on the 
HD IMap is not supported.

The `HDOrderedConcurrentIndexStore` and `HDUnorderedConcurrentIndexStore` classes are introduced to support `global` concurrent
ordered and unordered index stores.

In 4.1 release the index stores are used as a default for both `SORTED` and `HASH` global indexes on IMap data structure.

However, we still support old partitioned HD indexes, and the user can enable them setting
the `ClusterProperty#GLOBAL_HD_INDEX_ENABLED` property to `false`. The flag is global and cannot be changed on the deployed
cluster. 

The old query engine works with both new global HD index and the partitioned index on HD IMap,
but the new SQL engine requires new global concurrent HD index.     

## Technical Design

### Memory Management
The B+tree data structure stores index entries in the nodes organized into the tree-like data structure.
The typical size of the node is equal to the operating system page size.
Although 4.1 doesn't support the persistence feature yet, we follow the same approach and use 8K node size.

We introduce a special index memory manager to handle memory for the B+tree nodes. A reason why we cannot 
rely on the existing `POOLED/STANDARD` memory manager is that a B+tree node can be accessed by a concurrent query 
iterator even if the node has been released.

To handle this situation we have to guarantee that: 

- The query iterator doesn't crash the JVM when it accesses a B+tree node, while a concurrent operation just released the node. 
  That is a reason why the `STANDARD` memory manager cannot be used.

- The released blocks can be merged by the `POOLED` memory manager, and although accessing such block may not 
  trigger a JVM crash, the header of the B+tree node might be overwritten and the B+tree locking mechanism may fail.

For these reasons the index memory manager pools fixed size blocks (equal to the B+tree node size) and doesn't merge
them into bigger blocks. 

Moreover, the first 16 bytes of the B+tree node's header store 2 `long` values - the `lock state` of the node, followed 
by the `sequence number`. The lock state represents the locking information and is used by the Lock Manager to handle 
concurrent access to the node. The `sequence number` is a monotonically increasing value updated on every node's update.
Even when the node is released and later reused, the value is not reset.
The `sequence number` is used to detect that the node has changed since the last access by the query iterator, and that the
query iterator has to be re-synchronized.

### Lock Manager
The Lock Manager (LM) handles locking requests from the B+tree operations. The locking granularity is a B+tree node that 
can be locked in either shared or exclusive mode. It stores the locking state in the first 8 bytes
of the header of the B+tree node. 

The HD Lock Manager  implements the following interface.

```java
/**
 * Represents an API of lock manager built for synchronization of off-heap data structures.
 */
interface LockManager {
    /**
     * Acquires the read lock.
     * <p>
     * Acquires the read lock if the write lock is not held by another caller and returns immediately.
     * <p>
     * If the write lock is held by another caller then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the read lock has been acquired.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void readLock(long lockAddr);
    /**
     * Acquires the write lock.
     * Acquires the write lock if neither the read nor write lock are held by another caller and returns immediately.
     * <p>
     * If the lock is held by another caller then the current thread becomes disabled for thread scheduling purposes
     * and lies dormant until the write lock has been acquired.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void writeLock(long lockAddr);
    /**
     * Upgrades from a read lock to the write lock if neither the read nor write lock are held by another caller
     * and returns immediately.
     * <p>
     * If either read or write lock is held by another caller then this method will return immediately with the
     * value {@code false}.
     * <p>
     * It is up to the caller to correctly use this method, so that the caller must already held the read lock.
     * If the read lock is held by another caller, the upgrade will succeed and may cause a deadlock or corruption
     * of the underlying data structure.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     * @return {@code true} if the lock was upgraded, {@code false} otherwise.
     */
    boolean tryUpgradeToWriteLock(long lockAddr);
    /**
     * Acquires the write lock only if neither the read nor write lock are held by another caller and returns immediately.
     * <p>
     * If either read or write lock is held by another caller then this method will return immediately with the
     * value {@code false}.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     * @return {@code true} if the lock was free and was acquired, {@code false} otherwise.
     */
    boolean tryWriteLock(long lockAddr);
    /**
     * Instantly acquires the write lock.
     * <p>
     * Acquires (and immediately releases) the write lock if neither the read nor write lock are held by another caller
     * and returns immediately.
     * <p>
     * The method is useful to prevent busy waits.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void instantDurationWriteLock(long lockAddr);
    /**
     * Releases the lock acquired previously by the caller.
     * <p>
     * It is up to the caller to pair correctly acquisition and release of the lock. Inappropriate call
     * of the releaseLock may release the lock acquired by another logical operation, what in turn may cause
     * a corruption of the underlying data structure.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void releaseLock(long lockAddr);
}
```

The locking state stores 3 components: 

| not used      | write waiters | read waiters   | users count  |
| ------------- | ------------- |--------------- | -------------|
| 2 bytes       | 2 bytes       | 2  bytes       | 2 bytes      |

- The low 2 bytes store the `users count` of the lock.
  The `0` users count indicates that there is no lock, `-1` value means exclusive lock is acquired,
  and the value greater than zero indicates the number of acquired shared locks.

- The next 2 bytes store the `read waiters` count.
- The next 2 bytes store the `write waiters` count.
  The high 2 bytes are not used and reserved for future needs.

 The HD LM uses on-heap `ReentrantLock` to support internally the `wait/notify` functionality to block operation 
 requesting not compatible lock, and notify waiting operation to unblock when other concurrent operation releases 
 a lock.
 
 The HD LM uses a striped approach to handle unlimited number of off-heap locks with a limited number
 of `ReentrantLock`s. The stripes count is equal to `available processors * 4`.

 The striped approach limits the number of on-heap objects instantiated by the LM,
 but may cause an unnecessary wakeup of the blocked caller. The `stripes count` parameter is a trade-off
 between a number of synchronization objects and unnecessary wakeup(s).

 The `readLock` and `writeLock` methods employ a 2 stages approach. On the first stage the spinning approach is
 used. It tries to acquire a lock using relatively cheap CAS operation and if it fails after 32
 attempts it proceeds with the second stage. On the second stage, the more expensive `ReentrantLock` is used.

In general, the LM favors writers over readers if both are awaiting for a lock.

The LM doesn't have internal deadlock detector, and it is up to the user to prevent deadlock scenarios.

The read and write locks are not reentrant.

### The B+tree implementation
The B+tree is tree-like data structure. Every node has a fixed size and stored in off-heap memory. 
There are 2 types of nodes: inner and leaf nodes. The inner node is a navigating layer to find an appropriate leaf node 
that contains an off-heap address to the value corresponding to the search key.

The basic structure of the B+tree node is as follows


| The B+tree node slots area                    | Header       |
| --------------------------------------------- | -------------|

The inner node's header has 4 fields:

- 8 bytes, the `lock state`. See the Lock Manager section for more information;
- 8 bytes, the `sequence number` - monotonically incrementing value; 
 - 1 byte, the `node level`; leaf node has 0 level, root node has the highest level;
 - 2 bytes, the `keys count`, the number of keys stored in the node;


 The leaf node's header has 6 fields:

 - 8 bytes, the `lock state`. See the Lock Manager section for more information;
 - 8 bytes, the `sequence number` - monotonically incrementing value; 
 - 1 byte, the `node level`; the leaf node always has 0 level. 
   The level is used to detect whether the node is a leaf. 
   For the leaf node the `level = 0`. The `level` is also used during top -> down traversal to
   detect whether the current node is a leaf. A node split operation may cause a cascading split. A cascading
   split algorithm uses the `level` and index key to identify the inner node to split.                     
 - 2 bytes, the `keys count`, the number of keys stored in the node;
 - 8 bytes, the address of the `forward` leaf node, `NULL_ADDRESS` if it doesn't exist;
 - 8 bytes, the address of the `backward` leaf node, `NULL_ADDRESS` if it doesn't exist.

The slot area contains contains multiple slots (B+tree index entries). The slot consists of 3 components:

- 8 bytes off-heap address of the `index key`.
- 8 bytes off-heap address of the `entry key`.
- 8 bytes off-heap address of the entry value (for the leaf node)
  or address of the child node (for the inner node).

That is, every slot is fixed size and takes 24 bytes.

In addition to the aforementioned, the slot may contain a `payload` that is fixed size and the size can be customized 
by the B+tree's user. In 4.1 release the global `HASH` indexes use the payload to store the index key hash value.   

For leaf nodes, the slot count is equal to the `keys count` stored in the node header.
For inner nodes, the slot count is equal to `keys count + 1`. That is, if `keys count = 0`,
the inner node has one slot pointing to the child node.

 The leaf node "owns" the index key component of the slot and on slot remove the index key is disposed.

 The inner node "owns" both index key and entry key components and on slot remove they are both disposed.

 To make the B+tree operations thread-safe, we use a lock coupling approach, that is, before accessing
 the B+tree node, it should be locked in a shared or exclusive mode (depending on the operation), and the child
 node should be locked before releasing a lock on the parent.

 The navigation through the B+tree nodes always goes from the root node to the leaf node and from the leaf node 
 to the forward leaf node. Navigating in other directions may cause a deadlock. 

 While the B+tree can increase the depth, the root node's address never changes. A root node is always an inner node
 and may contain no keys. In this case in contains only an address of the child node that might be either 
 an inner or a leaf node. 

 A valid B+tree index always has at least 1 level, that is, it always has at least one inner and one leaf node.

#### Node split

If a user insets a new key into the index, the appropriate leaf node might be full and in this case it should be splitted
into two nodes. At this state the parent and leaf nodes are write locked.

If the parent node has capacity to accommodate one more slot entry, the split procedure divides the full leaf into 2 leaf nodes
and moves half of the entries into the newly created leaf node. An appropriate pointer to the newly created node is added
to the parent. The forward and backward pointers to the neighbouring leafs are also updated.
The locks on the parent and leafs are released and then the insert procedure restarts. It is still possible that 
the leaf node is full (because of concurrent inserts) and then the split procedure repeats. 

However, if the parent node has no capacity to accommodate one more slot entry, the aforementioned split procedure cannot 
succeed and the split of the parent is required. It is done similar to the leaf node split. The procedure starts from the 
root node and navigates to the parent of the inner node to be splitted. The only difference with the leaf node split is that the 
forward/backward pointers don't exist between inner nodes.

An inner node split may require a split of the inner node one level up and so on up to the root node. We call such a scenario 
 as a cascading split. 

The root's split is a special procedure that doesn't actually split the node. Instead, a new inner node is created and the full content of the 
root is moved to this new node while the old root node completely removes all slot entries and only stores a pointer to the 
newly created node. The level of the root node increments and that is the only way how the depth of the B+tree changes.    

#### Iterator results batching
A lookup or range scan operation may produce a huge result set and to avoid `OOME` the HD B+tree implementation supports 
lazy iteration over result set so that only a small portion of the results pre-materializes before returning it to the user.

The 4.1 implementation supports iterator result batching so that internally iterator keeps the
pre-materialized results in on-heap memory. 

This helps to avoid two potential performance problems:

- It minimizes the `readLock/releaseLock` calls count on the leaf node. Without batching every `next()` call on the iterator 
  would require the current leaf node locking, checking that it has not changed, producing one on-heap result entry and
  releasing the leaf node. With batching this overhead is minimized.
- If iterator's current node has been changed, the iterator has to be re-synchronized, that means a navigation from the root node 
  to the appropriate new leaf node is required. The iterator batching minimizes this overhead as well.

In the current implementation the batch size is `1000`.    

#### Iterator consistency guarantees
First, assume the iterator batching is disabled.

In this case, the iterator cannot return duplicates and/or miss index entries if there are no concurrent entry mutations
related to the query criteria. It means the iterator safely survives the B+tree internal structure mutations caused by
un-related entry mutations.
If a related entry mutates, the iterator may return a duplicate or miss an entry. But this anomaly is beyond the scope 
of the B+tree implementation.

If the batching is enabled, the guarantees are similar. However, the iterator may return stale data, which is already removed.

The SQL engine supports for batching on the query engine level as well,
and the B+tree batching is not the only place where we are exposed to this anomaly.

### HASH index optimization
A `HASH` index is a special type of index that supports only one-value lookup operation while the range scan operations are 
not supported. 

In 4.1 we introduce a global HD `HASH` index based on the B+tree. The index keys in the `HASH` index are not ordered according 
to the natural keys ordering. Instead, the keys are ordered by hash-value of the index key. A leaf/inner node slot 
has a special payload that stores a hash-value of the index key. 

In this case the lookup operation doesn't de-serialize the index-key value for each comparison. It uses the hash-value from the 
payload for comparison and in case of hash collision, a map keys are compared byte-by-byte. This significantly reduces the 
de-serialization overhead.