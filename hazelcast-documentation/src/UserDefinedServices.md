

## User Defined Services

In the case of special/custom needs, Hazelcast's SPI (Service Provider Interface) module offers the users to develop their own distributed data structures and services.

### What Can Be Accomplished

List the most famous things that a user can do using SPI:

- ???
- ???
- ???


### Creating a Custom DDS
???Steps for creating a custom distributed data structure???



### Creating a Custom Service
???Steps for creating a custom service???

### Sample Case

Throughout this section, a distributed counter that we are going to create, will be the guide to reveal the usage of Hazelcast SPI.

Here is our counter.

```java
public interface Counter{
   int inc(int amount);
}
```

This counter will have the below features:
- It is planned to be stored in Hazelcast. 
- Different cluster members can call it. 
- It will be scalable, meaning that the capacity for the number of counters scales with the number of cluster members.
- It will be highly available, meaning that if a member hosting this counter goes down, a backup will be available on a different member.

All these features are going to be realized with the steps below. In each step, a new functionality to this counter is added.

1. Creating the class
2. Enabling the class
3. Adding properties
4. Starting the service
5. Placing a remote call
5. Creating a container
6. 



#### Creating the class

To have the counter as a functioning distributed object, we need a class. This class (namely CounterService in our sample) will be the gateway between Hazelcast internals and the counter, so that we can add features to the counter. In this step the CounterService is created. Its lifecycle will be managed by Hazelcast. 

`CounterService` should implement the interface `com.hazelcast.spi.ManagedService` as shown below.

```java
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CounterService implements ManagedService {
    private NodeEngine nodeEngine;

    @Override
    public void init( NodeEngine nodeEngine, Properties properties ) {
        System.out.println( "CounterService.init" );
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void shutdown( boolean terminate ) {
        System.out.println( "CounterService.shutdown" );
    }

    @Override
    public void reset() {
    }

}
```

As can be seen from the code, below methods are implemented. 

- `init`: This is called when this CounterService is initialized. `NodeEngine` enables access to Hazelcast internals such as `HazelcastInstance` and `PartitionService`. Also, the object `Properties` will provide us with creating our own properties.
- `shutdown`: This is called when CounterService is shutdown. It cleans up the resources.
- `reset`: This is called when cluster members face with Split-Brain issue. This occurs when disconnected members that have created their own cluster are merged back into the main cluster. Services can also implement the SplitBrainHandleService to indicate that they can take part in the merge process. For the CounterService we are going to implement as a no-op.

#### Enabling the Class

Now, we need to enable the class `CounterService`. Declarative way of doing this is shown below.

```xml
<network>
   <join><multicast enabled="true"/> </join>
</network>
<services>
   <service enabled="true">
      <name>CounterService</name>
      <class-name>CounterService</class-name>
   </service>
</services>
```

`CounterService` is declared within the `services` tag of configuration. 

- Setting the `enabled` attribute as `true` enables the service.
- `name` attribute defines the name of the service. It should be a unique name (`CounterService` in our case) since it will be looked up when a remote call is made. Note that, the value of this attribute will be sent at each request. So, a longer value means more data (de)serialization. A good practice is giving an understandable name with the shortest possible length.
- `class-name`: Class name of the service (`CounterService` in our case). Class should have a *no-arg* constructor. Otherwise, the object cannot be initialized.

Moreover, note that multicast is enabled as the join mechanism. In the later sections, we will see why.

#### Adding Properties

Remember that the `init` method takes `Properties` object as an argument. This means we can add properties to the service. These properties are passed to the method `init`. Adding properties can be done declaratively as shown below.

```xml
<service enabled="true">
   <name>CounterService</name>
   <class-name>CounterService</class-name>
   <properties> 
      <someproperty>10</someproperty>
   </properties>
</service>
```

If you want to parse a more complex XML, the interface `com.hazelcast.spi.ServiceConfigurationParser` can be used. It gives you access to the XML DOM tree.

#### Starting the Service

Now, let's start a HazelcastInstance as shown below, which will eagerly start the CounterService.


```java
import com.hazelcast.core.Hazelcast;

public class Member {
    public static void main(String[] args) {
        Hazelcast.newHazelcastInstance();
    }
}
```

Once it is started, below output will be seen.

`CounterService.init`

Once the HazelcastInstance is shutdown (for example with Ctrl+C), below output will be seen.

`CounterService.shutdown`

#### Placing a Remote Call - Proxy

Until so far, we accomplished only to start `CounterService` as part of a HazelcastInstance startup.

Now, let's connect the Counter interface to CounterService and perform a remote call to the cluster member hosting the counter data. Then, we are going to return a dummy result. 

Remote calls are performed via a proxy in Hazelcast. Proxies expose the methods at the client side. Once a method is called, proxy creates an operation object, sends this object to the cluster member responsible from executing that operation and then sends the result. 

First, we need to make the Counter interface a distributed object. It should extend the `DistributedObject` interface for this purpose, as shown below.


```java
import com.hazelcast.core.DistributedObject;

public interface Counter extends DistributedObject {
    int inc(int amount);
}
```

Now, we need to make the CounterService implementing not only the ManagedService interface, but also the interface `com.hazelcast.spi.RemoteService`. This way, a client will be able to get a handle of a counter proxy.


```java
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.Properties;

public class CounterService implements ManagedService, RemoteService {
    public static final String NAME = "CounterService";

    private NodeEngine nodeEngine;

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new CounterProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        // for the time being a no-op, but in the later examples this will be implemented
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public void reset() {
    }
}
```

The `CounterProxy` returned by the method `createDistributedObject` is a local representation to (potentially) remote managed data and logic.
<br></br>

***NOTE***

*Note that caching and removing the proxy instance are done outside of this service.*
<br></br>

Now, it is time to implement the `CounterProxy` as shown below.

```java
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public class CounterProxy extends AbstractDistributedObject<CounterService> implements Counter {
    private final String name;

    public CounterProxy(String name, NodeEngine nodeEngine, CounterService counterService) {
        super(nodeEngine, counterService);
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return CounterService.NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int inc(int amount) {
        NodeEngine nodeEngine = getNodeEngine();
        IncOperation operation = new IncOperation(name, amount);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(name);
        InvocationBuilder builder = nodeEngine.getOperationService()
                .createInvocationBuilder(CounterService.NAME, operation, partitionId);
        try {
            final Future<Integer> future = builder.invoke();
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
```


`CounterProxy` is a local representation of remote data/functionality, it does not include the counter state. So, the method `inc` should be invoked on the cluster member hosting the real counter. This invocation can be performed using Hazelcast SPI. It will send the operations to the correct member and return the results.

Let's dig deeper into the method `inc`.

- First, we create `IncOperation` with a given `name` and `amount`.
- Then, we get the partition ID based on the `name`; by this way, all operations for a given name will result in the same partition ID.
- Then, we create an `InvocationBuilder` where the connection between operation and partition is made.
- Finally, we invoke the `InvocationBuilder` and wait for its result. This waiting is performed simply with a `future.get()`. Of course, in our case, timeout is not important. However, it is a good practice to use a timeout for a real system since operations should be completed in a certain amount of time. 

Hazelcast's `ExceptionUtil` is a good solution when it comes to dealing with execution exceptions. When the execution of the operation fails with an exception, an `ExecutionException` is thrown and handled with the method `ExceptionUtil.rethrow(Throwable)`. 

If it is an `InterruptedException`, we have two options: Either propagating the exception or just using the `ExceptionUtil.rethrow` for all exceptions. Please see below sample.


```java
  try {
     final Future<Integer> future = invocation.invoke();
     return future.get();
  } catch(InterruptedException e){
     throw e;
  } catch(Exception e){
     throw ExceptionUtil.rethrow(e);
  }
```


Now, let's write the `IncOperation`. It implements `PartitionAwareOperation` interface, meaning that it will be executed on partition that hosts the counter.


```java
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

class IncOperation extends AbstractOperation implements PartitionAwareOperation {
    private String objectId;
    private int amount, returnValue;

    // Important to have a no-arg constructor for deserialization
    public IncOperation() {
    }

    public IncOperation(String objectId, int amount) {
        this.amount = amount;
        this.objectId = objectId;
    }

    @Override
    public void run() throws Exception {
        System.out.println("Executing " + objectId + ".inc() on: " + getNodeEngine().getThisAddress());
        returnValue = 0;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeInt(amount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        amount = in.readInt();
    }
}
```

The method `run` does the actual execution. Since `IncOperation` will return a response, the method `returnsResponse` returns `true`. If your method is asynchrononous and does not need to return a response, it is better to return `false` since it will be faster. Actual response is stored in the field `returnValue` field and it is retrieved by the method `getResponse`.

You see two other methods in the above code: `writeInternal` and `readInternal`. Since `IncOperation` needs to be serialized, these two methods should be overwritten. Hence, `objectId` and `amount` will be serialized and available when the operation is executed. For the deserialization, note that the operation must have a *no-arg* constructor.

Now, let's run our code.

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.UUID;

public class Member {
    public static void main(String[] args) {
        HazelcastInstance[] instances = new HazelcastInstance[2];
        for (int k = 0; k < instances.length; k++)
            instances[k] = Hazelcast.newHazelcastInstance();

        Counter[] counters = new Counter[4];
        for (int k = 0; k < counters.length; k++)
            counters[k] = instances[0].getDistributedObject(CounterService.NAME, k+"counter");

        for (Counter counter : counters)
            System.out.println(counter.inc(1));

        System.out.println("Finished");
        System.exit(0);
    }
}
```

Once run, you will see the output as below.

`Executing 0counter.inc() on: Address[192.168.1.103]:5702`

`0`

`Executing 1counter.inc() on: Address[192.168.1.103]:5702`

`0`

`Executing 2counter.inc() on: Address[192.168.1.103]:5701`

`0`

`Executing 3counter.inc() on: Address[192.168.1.103]:5701`

`0`

`Finished`

As you see, counters are stored in different cluster members. Also note that, increment is not in its real action for now since the value remains as **0**. 

So, until now, we have made the basics up and running. In the next section, we will make the counter   

In this example we managed to get the basics up and running, but there are somethings not correctly implemented. For example, in the current code always a new proxy is returned instead of a cached one. And also the destroy is not correctly implemented on the CounterService. In the next examples we'll see these issues resolved.

#### Container

In this section we are going to upgrade the functionality so that it features a real distributed counter. So some kind of data-structure will hold an integer value and can be incremented and we'll also cache the proxy instances and deal with proxy instance destruction.

The first thing we do is for every partition there is in the system, we are going to create a Container which will contain all counters and proxies for a given partition:

```java
import java.util.HashMap;
import java.util.Map;

class Container {
    private final Map<String, Integer> values = new HashMap();

    int inc(String id, int amount) {
        Integer counter = values.get(id);
        if (counter == null) {
            counter = 0;
        }
        counter += amount;
        values.put(id, counter);
        return counter;
    }
    
    public void init(String objectName) {
        values.put(objectName,0);
    }

    public void destroy(String objectName) {
        values.remove(objectName);
    }
    
    ...
}
```

Hazelcast will give the guarantee that within a single partition, only a single thread will be active. So we don't need to deal with concurrency control while accessing a container.

For this chapter I rely on Container instance per partition, but you have complete freedom how to go about it. A different approach used in the Hazelcast is that the Container is dropped and the CounterService has a map of counters:

```java
final ConcurrentMap<String, Integer> counters = 
   new ConcurrentHashMap<String, Integer>();
```

The id of the counter can be used as key and an Integer as value. The only thing you need to take care of is that if operations for a specific partition are executed, you only select the values for that specific partition. This can be as simple as: 

```java
for(Map.Entry<String,Integer> entry: counters.entrySet()){
   String id = entry.getKey();
   int partitinId = nodeEngine.getPartitionService().getPartitionId(id); 
   if(partitionid == requiredPartitionId){
      ...do operation	
   }
}
```


Its a personal taste which solution is preferred. Personally I like the container approach since there will not be any mutable shared state between partitions. It also makes operations on partitions simpler since you don't need to filter out data that doesn't belong to a certain partition.

The next step is to integrate the Container in the CounterService:

```java
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CounterService implements ManagedService, RemoteService {
    public final static String NAME = "CounterService";
    Container[] containers;
    private NodeEngine nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        containers = new Container[nodeEngine.getPartitionService().getPartitionCount()];
        for (int k = 0; k < containers.length; k++)
            containers[k] = new Container();
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public CounterProxy createDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.init(objectName);
        return new CounterProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.destroy(objectName);
    }

    @Override
    public void reset() {
    }

    public static class Container {
        final Map<String, Integer> values = new HashMap<String, Integer>();

        private void init(String objectName) {
            values.put(objectName, 0);
        }

        private void destroy(String objectName){
            values.remove(objectName);
        }
    }
}
```
    

In the 'init' method you can see that a container is created for every partition. The next step is 'createDistributedObject'; apart from creating the proxy, we also initialize the value for that given
proxy to '0', so that we don't run into a NullPointerException. The last part is the 'destroyDistributedObject' where the value for the object is removed. If we don't clean up, we'll end up with memory that isn't remove and potentially can lead to an OOME.

The last step is connecting the IncOperation.run to the container:

```java
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;

class IncOperation extends AbstractOperation implements PartitionAwareOperation {
    private String objectId;
    private int amount, returnValue;

    public IncOperation() {
    }

    public IncOperation(String objectId, int amount) {
        this.amount = amount;
        this.objectId = objectId;
    }

    @Override
    public void run() throws Exception {
        System.out.println("Executing " + objectId + ".inc() on: " + getNodeEngine().getThisAddress());
        CounterService service = getService();
        CounterService.Container container = service.containers[getPartitionId()];
        Map<String, Integer> valuesMap = container.values;

        Integer counter = valuesMap.get(objectId);
        counter += amount;
        valuesMap.put(objectId, counter);
        returnValue = counter;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeInt(amount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        amount = in.readInt();
    }
}
```

The container can easily be retrieved using the partitionId since the range or partitionId's is 0 to partitionCount (exclusive) so it can be used as an index on the container array. Once the container has been retrieved we can access the value. In this example I have moved all 'inc' logic in the IncOperation, but it could be that you prefer to move the logic to the CounterService or to the Partition. 

When we run the following example code:

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Member {
    public static void main(String[] args) {
        HazelcastInstance[] instances = new HazelcastInstance[2];
        for (int k = 0; k < instances.length; k++)
            instances[k] = Hazelcast.newHazelcastInstance();

        Counter[] counters = new Counter[4];
        for (int k = 0; k < counters.length; k++)
            counters[k] = instances[0].getDistributedObject(CounterService.NAME, k+"counter");

        System.out.println("Round 1");
        for (Counter counter: counters)
            System.out.println(counter.inc(1));

        System.out.println("Round 2");
        for (Counter counter: counters)
            System.out.println(counter.inc(1));

        System.out.println("Finished");
        System.exit(0);
    }
}
```

We get the following output:

```
Round 1
Executing 0counter.inc() on: Address[192.168.1.103]:5702
1
Executing 1counter.inc() on: Address[192.168.1.103]:5702
1
Executing 2counter.inc() on: Address[192.168.1.103]:5701
1
Executing 3counter.inc() on: Address[192.168.1.103]:5701
1
Round 2
Executing 0counter.inc() on: Address[192.168.1.103]:5702
2
Executing 1counter.inc() on: Address[192.168.1.103]:5702
2
Executing 2counter.inc() on: Address[192.168.1.103]:5701
2
Executing 3counter.inc() on: Address[192.168.1.103]:5701
2
Finished
```


This means that we now have a basic distributed counter up and running.

#### Partition Migration

In our previous phase we managed to create real distributed counters. The only problem is that when members are leaving or joining the cluster, that the content of the partition containers is not migrating to different members. In this section we are going to just that: partition migration.

The first thing we are going to do is to add 3 new operations to the Container:


```java
import java.util.HashMap;
import java.util.Map;

class Container {
    private final Map<String, Integer> values = new HashMap();

    int inc(String id, int amount) {
        Integer counter = values.get(id);
        if (counter == null) {
            counter = 0;
        }
        counter += amount;
        values.put(id, counter);
        return counter;
    }

    void clear() {
        values.clear();
    }

    void applyMigrationData(Map<String, Integer> migrationData) {
        values.putAll(migrationData);
    }

    Map<String, Integer> toMigrationData() {
        return new HashMap(values);
    }

    public void init(String objectName) {
        values.put(objectName,0);
    }

    public void destroy(String objectName) {
        values.remove(objectName);
    }
}
```

- `toMigrationData`: this method is going to be called when Hazelcast wants to get started with the migration of the partition on the member that currently owns the partition. The result of the toMigrationData is data of the partition in a form so that it can be serialized to another member.
- `applyMigrationData`: this method is called when the migrationData that is created by the toMigrationData method is going to be applied to member that is going to be the new partition owner.
- `clear`: is going to be called for 2 reasons: when the partition migration has succeeded and the old partition owner can get rid of all the data in the partition. And also when the partition migration operation fails and given the 'new' partition owner needs to roll back its changes.


The next step is to create a CounterMigrationOperation that will be responsible for transferring the migrationData from one machine to another and to call the 'applyMigrationData' on the correct partition of the new partition owner.

```java
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CounterMigrationOperation extends AbstractOperation {

    Map<String, Integer> migrationData;

    public CounterMigrationOperation() {
    }

    public CounterMigrationOperation(Map<String, Integer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CounterService service = getService();
        Container container = service.containers[getPartitionId()];
        container.applyMigrationData(migrationData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Integer> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new HashMap<String, Integer>();
        for (int i = 0; i < size; i++)
            migrationData.put(in.readUTF(), in.readInt());
    }
}
```

During the execution of a migration no other operations will be running in that partition. So you don't need to deal with thread-safety.

The last part is connecting all the pieces. This is done by adding an additional MigrationAwareService interface on the CounterService which will signal Hazelcast that our service is able to participate in partition migration:

```java
import com.hazelcast.core.DistributedObject;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.*;

import java.util.Map;
import java.util.Properties;

public class CounterService implements ManagedService, RemoteService, MigrationAwareService {
    public final static String NAME = "CounterService";
    Container[] containers;
    private NodeEngine nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        containers = new Container[nodeEngine.getPartitionService().getPartitionCount()];
        for (int k = 0; k < containers.length; k++)
            containers[k] = new Container();
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.init(objectName);
        return new CounterProxy(objectName, nodeEngine,this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.destroy(objectName);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent e) {
        //no-op
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        Container container = containers[partitionId];
        container.clear();
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent e) {
        if (e.getReplicaIndex() > 1) {
            return null;
        }
        Container container = containers[e.getPartitionId()];
        Map<String, Integer> data = container.toMigrationData();
        return data.isEmpty() ? null : new CounterMigrationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent e) {
        if (e.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            Container c = containers[e.getPartitionId()];
            c.clear();
        }

        //todo
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent e) {
        if (e.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            Container c = containers[e.getPartitionId()];
            c.clear();
        }
    }

    @Override
    public void reset() {
    }
}
```


By implementing the MigrationAwareService some additional methods are exposed:

- `beforeMigration`: this is called before we any migration is done. It is useful if you need to .. In our case we do nothing.
- `prepareMigrationOperation`: method will return all the data of the partition that is going to be moved.
- `commitMigration`: commits the data. In this case committing means that we are going to clear the container for the partition of the old owner. Even though we don't have any complex resources like threads, database connections etc, clearing the container is advisable to prevent memory issues.is this method called on both the primary and backup? [mehmet: yes] if this node is source side of migration (means partition is migrating FROM this node) and migration type is MOVE (means partition is migrated completely not copied to a backup node) then remove partition data from this node. If this node is destination or migration type is copy then nothing to do.is this method called on both the primary and backup? [mehmet: yes][mehmet: if this node is destination side of migration (means partition is migrating TO this node) then remove partition data from this node.If this node is source then nothing to do.

- `rollbackMigration`: ???


```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Member {
    public static void main(String[] args) throws Exception {
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int k = 0; k < instances.length; k++)
            instances[k] = Hazelcast.newHazelcastInstance();

        Counter[] counters = new Counter[4];
        for (int k = 0; k < counters.length; k++)
            counters[k] = instances[0].getDistributedObject(CounterService.NAME, k + "counter");

        for (Counter counter : counters)
            System.out.println(counter.inc(1));

        Thread.sleep(10000);

        System.out.println("Creating new members");

        for (int k = 0; k < 3; k++) {
            Hazelcast.newHazelcastInstance();
        }

        Thread.sleep(10000);

        for (Counter counter : counters)
            System.out.println(counter.inc(1));

        System.out.println("Finished");
        System.exit(0);
    }
}
```

If we execute the example, we'll get output like this:

```
Executing 0counter.inc() on: Address[192.168.1.103]:5702
Executing backup 0counter.inc() on: Address[192.168.1.103]:5703
1
Executing 1counter.inc() on: Address[192.168.1.103]:5703
Executing backup 1counter.inc() on: Address[192.168.1.103]:5701
1
Executing 2counter.inc() on: Address[192.168.1.103]:5701
Executing backup 2counter.inc() on: Address[192.168.1.103]:5703
1
Executing 3counter.inc() on: Address[192.168.1.103]:5701
Executing backup 3counter.inc() on: Address[192.168.1.103]:5703
1
Creating new members
Executing 0counter.inc() on: Address[192.168.1.103]:5705
Executing backup 0counter.inc() on: Address[192.168.1.103]:5703
2
Executing 1counter.inc() on: Address[192.168.1.103]:5703
Executing backup 1counter.inc() on: Address[192.168.1.103]:5704
2
Executing 2counter.inc() on: Address[192.168.1.103]:5705
Executing backup 2counter.inc() on: Address[192.168.1.103]:5704
2
Executing 3counter.inc() on: Address[192.168.1.103]:5704
Executing backup 3counter.inc() on: Address[192.168.1.103]:5705
2
Finished
```

As you can see the counters have moved: '0counter' moved from from '192.168.1.103:5702' to '192.168.1.103:5705' but its is incremented correctly. So our counters are now able to move around in the cluster. If you need to have more capacity, add a machine and the counters are redistributed. If you have surplus capacity, shutdown the instance and the counters are redistributed.

#### Backups

In this last phase we are going to deal with backups; so make sure that when a member fails that the data of the counter is available on another node. This is done through replicating that change to another member in the cluster. With the SPI this can be done by letting the operation implement the com.hazelcast.spi.BackupAwareOperaton interface. Below you can see this interface being implemented
on the IncOperation:

```java
class IncOperation extends AbstractOperation 
	implements PartitionAwareOperation, BackupAwareOperation {
   ...   
   
   @Override
   public int getAsyncBackupCount() {
      return 0;
   }

   @Override
   public int getSyncBackupCount() {
      return 1;
   }

   @Override
   public boolean shouldBackup() {
      return true;
   }

   @Override
   public Operation getBackupOperation() {
      return new IncBackupOperation(objectId, amount);
   }
}
```

As you can see some additional methods need to be implemented. The getAsyncBackupCount and getSyncBackupCount signals how many asynchronous and synchronous backups there need to be. In our case we only want a single synchronous backup and no asynchronous backups. Here the number of backups is hard coded, but you could also pass the number of backups as parameters to the IncOperation, or by letting the methods access the CounterService. The shouldBackup method tells Hazelcast that our Operation needs a backup. In our case we are always going to make a backup; even if there is no change. But in practice you only want to make a backup if there is actually a change: in case of the IncOperation you want to make a backup if 'amount' is null. 

The last method is the 'getBackupOperation' which returns the actual operation that is going to make the backup; so the backup itself is an operation and will run on the same infrastructure. If a backup should be made and e.g. 'getSyncBackupCount' return 3, then 3 IncBackupOperation instances are created and send to the 3 machines containing the backup partition. If there are less machines available than backups need to be created, Hazelcast will just send a smaller number of operations. But it could be that a too small cluster, you don't get the same high availability guarantees you specified. 

So lets have a look at the IncBackupOperation:

```java
public class IncBackupOperation 
	extends AbstractOperation implements BackupOperation {
   private String objectId;
   private int amount;

   public IncBackupOperation() {
   }

   public IncBackupOperation(String objectId, int amount) {
      this.amount = amount;
      this.objectId = objectId;
   }

   @Override
   protected void writeInternal(ObjectDataOutput out) throws IOException {
      super.writeInternal(out);
      out.writeUTF(objectId);
      out.writeInt(amount);
   }

   @Override
   protected void readInternal(ObjectDataInput in) throws IOException {
      super.readInternal(in);
      objectId = in.readUTF();
      amount = in.readInt();
   }

   @Override
   public void run() throws Exception {
      CounterService service = getService();
      System.out.println("Executing backup " + objectId + ".inc() on: " 
        + getNodeEngine().getThisAddress());
      Container c = service.containers[getPartitionId()];
      c.inc(objectId, amount);
   }
}
```

Hazelcast will also make sure that a new IncOperation for that particular key will not be executing before the (synchronous) backup operation has completed.

Of course we also want to see the backup functionality in action.

```java
public class Member {
   public static void main(String[] args) throws Exception {
      HazelcastInstance[] instances = new HazelcastInstance[2];
      for (int k = 0; k < instances.length; k++) 
         instances[k] = Hazelcast.newHazelcastInstance();
    
      Counter counter = instances[0].getDistributedObject(CounterService.NAME, "counter");
      counter.inc(1);
      System.out.println("Finished");
      System.exit(0);
    }
}
```

When we run this Member we'll get the following output:

```
Executing counter0.inc() on: Address[192.168.1.103]:5702
Executing backup counter0.inc() on: Address[192.168.1.103]:5701
Finished
```

As you can see, not only the IncOperation has executed, also the backup operation is executed. You can also see that the operations have been executed on different cluster members to guarantee high availability. One of the experiments you could do it to modify the test code so you have a cluster of members in different JVMs and see what happens with the counters when you kill one of the JVM's. 