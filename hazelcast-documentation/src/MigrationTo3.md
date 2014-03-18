## Upgrading from 2.x versions


In this section, we list the changes what users should take into account before upgrading to latest Hazelcast from earlier versions.

-   **Removal of deprecated static methods**:
    The static methods of Hazelcast class reaching Hazelcast data components have been removed. The functionality of these methods can be reached from HazelcastInstance interface. Namely you should replace following:

	```java
Map<Integer, String> mapCustomers = Hazelcast.getMap("customers");
```

	with

	```java
HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
// or if you already started an instance
// HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName("instance1");
Map<Integer, String> mapCustomers = instance.getMap("customers");
```
-   **Removal of lite members**:
    With 3.0 there will be no member type as lite member. As 3.0 clients are smart client that they know in which node the data is located, you can replace your lite members with native clients.

-   **Renaming "instance" to "distributed object"**:
    Before 3.0 there was a confusion for the term "instance". It was used for both the cluster members and the distributed objects (map, queue, topic, etc. instances). Starting 3.0, the term instance will be only used for Hazelcast instances, namely cluster members. We will use the term "distributed object" for map, queue, etc. instances. So you should replace the related methods with the new renamed ones. As 3.0 clients are smart client that they know in which node the data is located, you can replace your lite members with native clients.

	```java
public static void main(String[] args) throws InterruptedException {
    Config cfg = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
    IMap map = hz.getMap("test");
    Collection<Instance> instances = hz.getInstances();
    for (Instance instance : instances) {
        if(instance.getInstanceType() == Instance.InstanceType.MAP) {
        System.out.println("there is a map with name:"+instance.getId());
        }
    }
}
```

	with

	```java
public static void main(String[] args) throws InterruptedException {
    Config cfg = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
    IMap map = hz.getMap("test");
    Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
        for (DistributedObject distributedObject : distributedObjects) {
            if(distributedObject instanceof IMap)
            System.out.println("there is a map with name:"+distributedObject.getName());
        }
}
```

-   **Package structure change**:
    PartitionService has been moved to package `com.hazelcast.core` from `com.hazelcast.partition`.


-   **Listener API change**:
    Before 3.0, `removeListener` methods was taking the Listener object as parameter. But, it causes confusion as same listener object may be used as parameter for different listener registrations. So we have changed the listener API. `addListener` methods return you an unique ID and you can remove listener by using this ID. So you should do following replacement if needed:

	```java
IMap map = instance.getMap("map");
map.addEntryListener(listener,true);
map.removeEntryListener(listener);
``` 
   
	with
	
	```java
IMap map = instance.getMap("map");
String listenerId = map.addEntryListener(listener, true);
map.removeEntryListener(listenerId);
```
-   **IMap changes**:
    -   `tryRemove(K key, long timeout, TimeUnit timeunit)` returns boolean indicating whether operation is successful.

    -   `tryLockAndGet(K key, long time, TimeUnit timeunit)` is removed.

    -   `putAndUnlock(K key, V value)` is removed.

    -   `lockMap(long time, TimeUnit timeunit)` and `unlockMap()` are removed

    -   `getMapEntry(K key)` is renamed as `getEntryView(K key)`. The returned object's type, MapEntry class is renamed as EntryView.

    -   There is no predefined names for merge policies. You just give the full class name of the merge policy implementation.

		```xml
<merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
```

	Also MergePolicy interface has been renamed to MapMergePolicy and also returning null from the implemented `merge()` method causes the existing entry to be removed.

-   **IQueue changes**:
    There is no change on IQueue API but there are changes on how IQueue is configured. With Hazelcast 3.0 there will not be backing map configuration for queue. Settings like backup count will be directly configured on queue config. For queue configuration details, please see [Distributed Queue](#distributed-queue) page.

-   **Transaction API change**:
    In Hazelcast 3.0, transaction API is completely different. Please see [Distributed Transactions](#transactions).

-   **ExecutorService API change**:
    Classes MultiTask and DistributedTask have been removed. All the functionality is supported by the newly presented interface IExecutorService. Please see [Distributed Execution](#distributed-execution).

-   **LifeCycleService API** has been simplified. `pause()`, `resume()`, `restart()` methods have been removed.

-   AtomicNumber class has been renamed to **IAtomicLong**.

-   **ICountDownLatch await()** operation has been removed. We expect users to use `await()` method with timeout parameters.

-   **ISemaphore API** has been substantially changed. `attach()`, `detach()` methods have been removed.
