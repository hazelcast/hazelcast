#### Backup of ICache Config

```xml
<cache>
  <!-- ... default cache configuration goes here ... -->
  <backup-count>1</backup-count>
  <async-backup-count>1</async-backup-count>
  <in-memory-format>BINARY</in-memory-format>
  <eviction size="10000" size-policy="ENTRY-COUNT" eviction-policy="LRU">
    <eviction-strategy-factory
        class-name="com.hazelcast.cache...sampling.SamplingBasedEvictionStrategyFactory"/>
    <eviction-policy-strategy-factory
        class-name="com.hazelcast.cache...policies.LRUEvictionPolicyStrategyFactory"/>
  </eviction>
</cache>
```

- `backup-count`: The number of synchronous backups. Those backups are executed before the mutating cache operation is finished. The mutating operation is blocked. `backup-count` default value is 1.
- `async-backup-count`: The number of asynchronous backups. Those backups are executed asynchronously so the mutating operation is not blocked and it will be done immediately. `async-backup-count` default value is 0.
- `in-memory-format`: Defines the internal storage format. For more information, please see the [In Memory Format section](#in-memory-format). Default is `BINARY`.
- `eviction`: Defines the used eviction strategies and sizes for the cache. For more information on eviction, please see the [JCache Eviction](#jcache-eviction).
  - `size`: The maximum number of records or maximum size in bytes depending on the `size-policy` property. Size can be any integer between `0` and `Integer.MAX_VALUE`. Default size-policy is `ENTRY_COUNT` and default size is `10.000`.
  - `size-policy`: The size policy property defines a maximum size. If maximum size is reached, the cache is evicted based on the eviction policy. Default size-policy is `ENTRY_COUNT` and default size is `10.000`. The following eviction policies are available:
    - `ENTRY_COUNT`: Maximum number of cache entries in the cache. **Available on heap based cache record store only.**
    - `USED_NATIVE_MEMORY_SIZE`: Maximum used native memory size in megabytes for each instance. **Available on High-Density Memory cache record store only.**
    - `USED_NATIVE_MEMORY_PERCENTAGE`: Maximum used native memory size percentage for each instance. **Available on High-Density Memory cache record store only.**
    - `FREE_NATIVE_MEMORY_SIZE`: Maximum free native memory size in megabytes for each instance. **Available on High-Density Memory cache record store only.**
    - `FREE_NATIVE_MEMORY_PERCENTAGE`: Maximum free native memory size percentage for each instance. **Available on High-Density Memory cache record store only.**
  - `eviction-policy`:
- `eviction-strategy-factory`: The full canonical class-name for a `javax.cache.configuration.Factory` to create an implementation of `com.hazelcast.cache.impl.eviction.EvictionStrategy` to select eviction candidates to apply the `com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy` implementation to. The default created implementation is `SamplingBasedEvictionStrategy` which samples a fixed number of random elements from the underlying partition storage.
- `eviction-policy-strategy-factory`: The full canonical class-name for a `javax.cache.configuration.Factory` to create an implementation of `com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy` to compare eviction candidates and select the best matching prospect for eviction. This is the actual eviction policy implementation. The default created implementation is `LRUEvictionPolicyStrategy` which itself implements an `LRU` (Less Recently Used) behavior.

#### Backup of Eviction Policies

Hazelcast JCache provides two commonly known eviction policies, LRU and LFU, but loosens the rules for predictable runtime
behavior. LRU normally recognized as `Least Recently Used` is implemented as `Less Recently Used` and LRU as
`Less Frequently Used`. The details about this difference is explained later in this chapter,
[Eviction Algorithm](#eviction-algorithm).

Eviction Policies are defined by providing the corresponding `javax.cache.configuration.Factory` implementation to the
configuration as shown in the configuration section, [ICache Configuration](#icache-configuration). As already mentioned, two
built-in policies are available:

The full canonical class-name of the LRU (Less Recently Used) policy is:

```plain
com.hazelcast.cache.impl.eviction.impl.policies.LRUEvictionPolicyStrategyFactory
```

And the full canonical class-name of the LFU (Less Frequently Used) policy is:

```plain
com.hazelcast.cache.impl.eviction.impl.policies.LFUEvictionPolicyStrategyFactory
```

The default eviction policy is LRU, therefore Hazelcast JCache does not offer the possibility to perform no eviction. Users are
free to implement such a policy on their own but it is highly discouraged.

Apart from the built-in eviction policies, as already mentioned, users are able to define their own policies based on their
needs. The [Eviction SPI](#eviction-spi) is currently marked as BETA and provides a preview of the final common eviction SPI.
Users that implement their own policies might have to slightly change their implementation in the future.

#### Backup of Eviction Strategy

Eviction strategies implement the logic to select one or more eviction candidates from the underlying storage implementation and
pass them to the eviction policies. Hazelcast JCache provides a amortized O(1) cost implementation for this strategy to select a
fixed number of samples from the current partition that it is executed against.

The default implementation is `com.hazelcast.cache.impl.eviction.impl.sampling.SamplingBasedEvictionStrategy` which, as mentioned,
samples random 15 elements. A detailed description of the algorithm will be explained in the next section.

As for eviction policies, users are able to implement their own `EvictionStrategy` implementation by using the preview of the
[Eviction SPI](#eviction-spi). It is highly recommended to keep constant runtime behavior and high throughput in mind while
designing a custom implementation!

#### Eviction SPI

The Eviction SPI is a set of interfaces and abstract classes to support implementations of eviction algorithms in a more standard
and user definable way. The complete Hazelcast JCache eviction is built on top of this SPI to proof the usability of the SPI
itself and to provide a full fledged set of functionality to the user.

Implementations are offered by `javax.cache.configuration.Factory` to the system which are able to create instances of strategies,
policies and custom features or implementations.

Hazelcast JCache itself offers a set of built-in implementations which were already explained above. Please find the details for
those implementations in [Eviction Algorithm](#eviction-algorithm).

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *The Eviction SPI is currently a preview of a upcoming standard SPI for implementing user defined eviction strategies and eviction
policies. Keep in mind that, as a preview, this SPI classes and interfaces might be subject of change in later versions of
Hazelcast.*
<br></br>

##### Evictable


An `com.hazelcast.cache.impl.eviction.Evictable` is the basic element of the eviction system. The `Evictable` interface defines
a set of properties to identify if an instance is expired or to retrieve timestamps or values to predict the qualification for
eviction.

```java
interface Evictable {
  /**
   * Gets the creation time of this Evictable in milliseconds.
   */
  long getCreationTime();

  /**
   * Gets the latest access time difference of this Evictable in
   * milliseconds.
   */
  long getLastAccessTime();

  /**
   * Gets the access hit count of this Evictable.
   */
  int getAccessHits();

  /**
   * Returns true if the entry is expired at the given timestamp, otherwise
   * false. Normally the given timestamp is the current point in time but is
   * not required to be.
   *
  boolean isExpired(long timestamp);
}
```

##### EvictionCandidate

The `com.hazelcast.cache.impl.eviction.EvictionCandidate` interface describes a possible candidate to be evicted from a subset
of `Evictable`s in the storage. The internal implementation of Hazelcast extends the HashEntry class of the underlying used
`ConcurrentReferenceHashMap` to prevent creation of new objects.

```java
interface EvictionCandidate<A, E extends Evictable> {
  /**
   * The accessor is any kind of access key to make it able for an
   * EvictableStore to remove a EvictionCandidate from the internal
   * storage data structure.
   */
  A getAccessor();

  /**
   * The returned Evictable is the actual candidate of eviction to
   * decide on to be evicted or not.
   */
  E getEvictable();
}
```

##### EvictionPolicyStrategy

An `com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy` interface is meant to implement the actual eviction policy, such as
LRU or LFU. The default implementations of Hazelcast are optimized for the commonly used partition threading system of Hazelcast
as described in the [Threading](#threading-model) chapter.

```java
interface EvictionPolicyStrategy<A, E extends Evictable> {
  /**
   * The apply method implements the actual policy rules and is called
   * on every eviction to select one or more candidates to be evicted
   * from the given input set of EvictionCandidates.
   * The selection algorithm should execute in a constant time to
   * deliver a predictable timing results of the eviction system.
   */
  Iterable<EvictionCandidate<A, E>> evaluate(
      Iterable<EvictionCandidate<A, E>> evictionCandidates );

  /**
   * A hook method that is called when new entries are created on the
   * passed EvictableStore instance.
   * This method is meant to be used to update internal state of
   * EvictionStrategy and also has to delegate the call to the given
   * EvictionPolicyStrategy.
   */
  void onCreation( Evictable evictable );

  /**
   * A hook method that is called when an entry is loaded on the passed
   * EvictableStore instance by an configured external resource like a
   * database storage or similar.
   * This method is meant to be used to update internal state of
   * EvictionStrategy and also has to delegate the call to the given
   * EvictionPolicyStrategy.
   */
  void onLoad( Evictable evictable );

  /**
   * A hook method that is called when entries are accessed (get) in
   * the passed EvictableStore instance.
   * This method is meant to be used to update internal state of
   * EvictionStrategy and also has to delegate the call to the given
   * EvictionPolicyStrategy.
   */
  void onRead( Evictable evictable );

  /**
   * A hook method that is called when entries are updated in the passed
   * EvictableStore instance. This method is meant to be used to update
   * internal state of EvictionStrategy and also has to delegate the call
   * to the given EvictionPolicyStrategy.
   */
  void onUpdate( Evictable evictable );

  /**
   * A hook method that is called when entries are removed in the passed
   * EvictableStore instance.
   * This method is meant to be used to update or cleanup internal state of
   * EvictionStrategy and also has to delegate the call to the given
   * EvictionPolicyStrategy.
   */
  void onRemove( Evictable evictable );

  /**
   * The configure method is called after constructing the instance to pass
   * in properties defined by the user. Configuration properties might be
   * different from each other and are meant to be implementation specific.
   */
  void configure( Properties properties );
}
```

For implementations with no need to store internal state there is an abstract base class which just implements those hook methods
as no-op operations.

```java
abstract class AbstractEvictionPolicyStrategy<A, E extends Evictable>
    implements EvictionPolicyStrategy<A, E> {

    // ... no-op implementations for onX hook methods
}
```

##### EvictableStore

The `com.hazelcast.cache.impl.eviction.EvictableStore` interface describes a base set of methods for a storage implementation
that is able to interact with the eviction system to evict elements from the internal state. It also provides information about
its internal state such as number of entries or the entries memory consumption.

```java
interface EvictableStore<A, E extends Evictable> {
  /**
   * The evict method is called by the EvictionStrategy to eventually
   * evict, by the EvictionPolicyStrategy, selected EvictionCandidates
   * from the internal data structures.
   */
  int evict( Iterable<EvictionCandidate<A, E>> evictionCandidates );

  /**
   * Returns the current count of the partition.
   */
  long getPartitionEntryCount();

  /**
   * Returns the defined capacity over all partitions in the cluster
   * for that data structure. This is assumed to be equal to
   * max := sum(partitionSize) in partitioned data structures.
   */
  long getGlobalEntryCapacity();

  /**
   * Returns the memory consumption of the current partition in bytes.
   * This is an optional feature. If the underlying storage engine
   * doesn't support that feature it has to return -1.
   */
  long getPartitionMemoryConsumption();
}
```

##### EvictionStrategy

The interface `com.hazelcast.cache.impl.eviction.EvictionStrategy` defines the actual strategy to select a subset of entries from
the underlying data storage. An implementation of the evict method is meant to run in constant time and to execute in a reasonable
short time. The hook methods to update internal state of the `EvictionStrategy` must also be delegated to the passed
`EvictionPolicyStrategy`.

```java
interface EvictionStrategy<A, E extends Evictable,
                           S extends EvictableStore<A, E>> {
  /**
   * The evict method is called by a storage to trigger eviction from the
   * passed in EvictableStore. It is up to the implementation to provide a
   * full set of entries as candidates to the EvictionPolicyStrategy or to
   * select just a subset to speed up eviction.
   */
  int evict( S evictableStore,
             EvictionPolicyStrategy<A, E> evictionPolicyStrategy,
             EvictionEvaluator<A, E, S> evictionEvaluator );

  /**
   * A hook method that is called when new entries are created on the passed
   * EvictableStore instance.
   */
  void onCreation( S evictableStore,
                   EvictionPolicyStrategy evictionPolicyStrategy,
                   Evictable evictable );

  /**
   * A hook method that is called when an entry is loaded on the passed
   * EvictableStore instance by an configured external resource like a
   * database storage or similar.
   */
  void onLoad( S evictableStore,
               EvictionPolicyStrategy evictionPolicyStrategy,
               Evictable evictable );

  /**
   * A hook method that is called when entries are accessed (get) in the
   * passed EvictableStore instance.
   */
  void onRead( S evictableStore,
               EvictionPolicyStrategy evictionPolicyStrategy,
               Evictable evictable );

  /**
   * A hook method that is called when entries are updated in the passed
   * EvictableStore instance.
   */
  void onUpdate(S evictableStore, EvictionPolicyStrategy evictionPolicyStrategy,
      Evictable evictable);

  /**
   * A hook method that is called when entries are removed in the passed
   * EvictableStore instance.
   */
  void onRemove( S evictableStore,
                 EvictionPolicyStrategy evictionPolicyStrategy,
                 Evictable evictable );
}
```

For implementations with no need to store internal state there is an abstract base class which just implements those hook methods
as delegate only operations.

```java
abstract class AbstractEvictionStrategy<A, E extends Evictable,
                                        S extends EvictableStore<A, E>>
    implements EvictionStrategy<A, E, S> {

    // ... delegate implementations for onX hook methods
}
```

##### EvictionEvaluator

An `com.hazelcast.cache.impl.eviction.EvictionEvaluator` is used to implement logic to decide if an eviction should be executed
or not such as based on the state of an EvictableStore, such as element count or memory consumption.

```java
interface EvictionEvaluator<A, E extends Evictable,
                            S extends EvictableStore<A, E>> {
  /**
   * The isEvictionRequired method implementation will decide, based on the
   * passed EvictableStore if an eviction is necessary or not.
   */
  boolean isEvictionRequired( S evictableStore );
}
```

##### SampleableEvictableStore

The `com.hazelcast.cache.impl.eviction.impl.sampling.SampleableEvictableStore` is a special sub-interface of
`com.hazelcast.cache.impl.eviction.EvictableStore` which add methods to supports sampling random elements from an underlying
data storage. This is the implementation interface of the built-in sampling algorithm used to implement LRU and LFU based on
sampling a fixed number of elements in constant time. Users are also free to implement subclasses of this interface.

```java
interface SampleableEvictableStore<A, E extends Evictable>
    extends EvictableStore<A, E> {
  /**
   * The sample method is used to sample a number of entries (defined by the
   * samples parameter) from the internal data structure. This method should
   * be executed in a constant time to deliver predictable timing results of
   * the eviction system.
   */
  Iterable<EvictionCandidate<A, E>> sample( int sampleCount );

  /**
   * Some implementations might hold internal state whenever the sample
   * method is called. This method can be used to cleanup internal state
   * after eviction happened.
   */
  void cleanupSampling( Iterable<EvictionCandidate<A, E>> candidates );
}
```

