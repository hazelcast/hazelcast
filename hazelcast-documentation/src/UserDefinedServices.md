

## User Defined Services

In the case of special/custom needs, Hazelcast's SPI (Service Provider Interface) module offers the users to develop their own distributed data structures and services.


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

1. Create the class.
2. Enable the class.
3. Add properties.
5. Place a remote call.
5. Create the containers.
6. Enable partition migration.
6. Create the backups.



#### Create the Class

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

#### Enable the Class

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

#### Add Properties

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

#### Start the Service

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

#### Place a Remote Call - Proxy

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

The method `run` does the actual execution. Since `IncOperation` will return a response, the method `returnsResponse` returns `true`. If your method is asynchronous and does not need to return a response, it is better to return `false` since it will be faster. Actual response is stored in the field `returnValue` field and it is retrieved by the method `getResponse`.

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

So, until now, we have made the basics up and running. In the next section, we will make a more real counter, cache the proxy instances and deal with proxy instance destruction.


#### Create the Containers

First, let's create a Container for every partition in the system, that will contain all counters and proxies.


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

Hazelcast guarantees that a single thread will be active in a single partition. So, when accessing a container, concurrency control will not be an issue. 

This section uses Container instance per partition approach. In this way, there will not be any mutable shared state between partitions. It also makes operations on partitions simpler since you do not need to filter out data that does not belong to a certain partition.

Now, let's integrate the Container in the CounterService, as shown below.

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
    

As you see, a container for every partition is created with the method `init`. Then, we create the proxy with the method `createDistributedObject`. And finally, we need to remove the value of the object, using the method `destroyDistributedObject`. Otherwise, we may get OutOfMemory exception.


And now, as the last step in creating a Container, we will connect the method `IncOperation.run` to the Container, as shown below.

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

`partitionId` has a range between **0** and **partitionCount** and can be used as an index for the container array. Therefore, `partitionId` can easily be used to retrieve the container. And once the container has been retrieved, we can access the value. 

Let's run the below sample code.

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

The output will be seen as follows.

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


Above output indicates that we have now a basic distributed counter up and running.

#### Partition Migration

In the previous section, we created a real distributed counter. Now, we need to make sure that the content of partition containers is migrated to different cluster members, when a member is joining to or leaving the cluster. To make this happen, first we need to add three new methods (`applyMigrationData`, `toMigrationData` and `clear`) to the Container, as shown below.



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

- `toMigrationData`: This is called when Hazelcast wants to start the partition migration from the member owning the partition. The result of this method is the partition data in a form so that it can be serialized to another member.
- `applyMigrationData`: This is called when `migrationData` (created by the method `toMigrationData`) is going to be applied to the member that is going to be the new partition owner.
- `clear`: This is called when the partition migration is successfully completed and the old partition owner gets rid of all data in the partition. This method is called also when the partition migration operation fails and to-be-the-new partition owner needs to roll back its changes.

After these three methods are added to the Container, we need to create a `CounterMigrationOperation` class that transfers `migrationData` from one member to another and calls the method `applyMigrationData` on the correct partition of the new partition owner. A sample is shown below.

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


<br></br>
***NOTE:*** *During a partition migration, no other operations are executed on the related partition.*
<br></br>

Now, we need to make our CounterService class to also implement  `MigrationAwareService` interface. By this way, Hazelcast knows that the CounterService will be able to perform partition migration. See the below code.

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


With the `MigrationAwareService` interface, some additional methods are exposed. For example, the method `prepareMigrationOperation` returns all the data of the partition that is going to be moved.

The method `commitMigration` commits the data, meaning in this case, clears the partition container of the old owner. 


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

Once we run the above code, the output will be seen as follows.

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

As it can be seen the counters have moved. `0counter` moved from *192.168.1.103:5702* to *192.168.1.103:5705* and it is incremented correctly. as a result, our counters are now able to move around in the cluster. You will see the the counters will be redistributed once you add or remove a cluster member.

#### Create the Backups

In this last phase, we make sure that the data of counter is available on another node when a member goes down. We need to have `IncOperation` class to implement `BackupAwareOperation` interface contained in SPI package. See the below code.

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

The methods `getAsyncBackupCount` and `getSyncBackupCount` specifies the count of asynchronous and synchronous backups. For our sample, it is just one synchronous backup and no asynchronous backups. In the above code, counts of backups are hard coded, but they can also be passed to `IncOperation` as parameters. 

The method `shouldBackup` specifies whether our Operation needs a backup or not. For our sample, it returns `true`, meaning the Operation will always have a backup even if there are no changes. Of course, in real systems, we want to have backups if there is a change. For `IncOperation` for example, having a backup when `amount` is null would be a good practice.

The method `getBackupOperation` returns the operation (`IncBackupOperation`) that actually performs the backup creation; as you noticed now, the backup itself is an operation and will run on the same infrastructure. 

If, for example, a backup should be made and `getSyncBackupCount` returns **3**, then three `IncBackupOperation` instances are created and sent to the three machines containing the backup partition. If there are less machines available, then backups need to be created. Hazelcast will just send a smaller number of operations. 

Now, let's have a look at the `IncBackupOperation`.

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

<br></br>

***NOTE:*** *Hazelcast will also make sure that a new IncOperation for that particular key will not be executed before the (synchronous) backup operation has completed.*

Let's see the backup functionality in action with the below code.

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

Once it is run, the following output will be seen.

```
Executing counter0.inc() on: Address[192.168.1.103]:5702
Executing backup counter0.inc() on: Address[192.168.1.103]:5701
Finished
```

As it can be seen, both `IncOperation` and `IncBackupOperation` are executed. Notice that these operations have been executed on different cluster members to guarantee high availability.