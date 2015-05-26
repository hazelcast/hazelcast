
# Distributed Computing

From Wikipedia: Distributed computing refers to the use of distributed systems to solve computational problems. In distributed computing, a problem is divided into many tasks, each of which is solved by one or more computers.

## Executor Service

One of the coolest features of Java 1.5 is the Executor framework, which allows you to asynchronously execute your tasks (logical units of work), such as database query, complex calculation, and image rendering.

### Executor Overview

The default implementation of this framework (`ThreadPoolExecutor`) is designed to run within a single JVM. In distributed systems, this implementation is not desired since you may want a task submitted in one JVM and processed in another one. Hazelcast offers `IExecutorService` for you to use in distributed environments: it implements `java.util.concurrent.ExecutorService` to serve the applications requiring computational and data processing power.

With `IExecutorService`, you can execute tasks asynchronously and perform other useful tasks. If your task execution takes longer than expected, you can cancel the task execution. Tasks should be `Serializable` since they will be distributed.

In the Java Executor framework, you implement tasks two ways: Callable or Runnable.

* Callable: If you need to return a value and submit to Executor, implement the task as `java.util.concurrent.Callable`.
* Runnable: If you do not need to return a value, implement the task as `java.util.concurrent.Runnable`.

#### Callable

In Hazelcast, when you implement a task as `java.util.concurrent.Callable` (a task that returns a value), you implement Callable and Serializable.

Below is an example of a Callable.

```java
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class SumTask
    implements Callable<Integer>, Serializable, HazelcastInstanceAware {
        
  private transient HazelcastInstance hazelcastInstance;

  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public Integer call() throws Exception {
    IMap<String, Integer> map = hazelcastInstance.getMap( "map" );
    int result = 0;
    for ( String key : map.localKeySet() ) {
      System.out.println( "Calculating for key: " + key );
      result += map.get( key );
    }
    System.out.println( "Local Result: " + result );
    return result;
  }
}
```

Another example is the Echo callable below. In its call() method, it returns the local member and the input passed in. Remember that `instance.getCluster().getLocalMember()` returns the local member and `toString()` returns the member's address (IP + port) in String form, just to see which member actually executed the code for our example. Of course, the `call()` method can do and return anything you like. 

```java
import java.util.concurrent.Callable;
import java.io.Serializable;

public class Echo implements Callable<String>, Serializable {
    String input = null;

    public Echo() {
    }

    public Echo(String input) {
        this.input = input;
    }

    public String call() {
        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        return instance.getCluster().getLocalMember().toString() + ":" + input;
    }
}
```java

To execute a task with the executor framework:

* Obtain an `ExecutorService` instance (generally via `Executors`).
* Submit a task which returns a `Future`. 
* After executing the task, you do not have to wait for the execution to complete, you can process other things. 
* When ready, use the `Future` object to retrieve the result as shown in the code example below.

Below, the Echo task is executed.

```java
ExecutorService executorService = Executors.newSingleThreadExecutor();
Future<String> future = executorService.submit( new Echo( "myinput") );
//while it is executing, do some useful stuff
//when ready, get the result of your execution
String result = future.get();
```

Please note that the Echo callable in the above code sample also implements a Serializable interface, since it may be sent to another JVM to be processed.

![image](images/NoteSmall.jpg) ***NOTE:*** *When a task is deserialized, HazelcastInstance needs to be accessed. To do this, the task should implement `HazelcastInstanceAware` interface. Please see the [HazelcastInstanceAware Interface section](#hazelcastinstanceaware-interface) for more information.*
<br></br>


#### Runnable

In Hazelcast, when you implement a task as `java.util.concurrent.runnable` (a task that does not return a value), you implement Callable and Serializable.

Below is Runnable example code. It is a task that waits for some time and echoes a message.

```java
public class EchoTask implements Runnable, Serializable {
  private final String msg;

  public EchoTask( String msg ) {
    this.msg = msg;
  }

  @Override
  public void run() {
    try {
      Thread.sleep( 5000 );
    } catch ( InterruptedException e ) {
    }
    System.out.println( "echo:" + msg );
  }
}
```

To execute the task:
* Retrieve the Executor from `HazelcastInstance`.
* Submit the tasks to the Executor.

Now let's write a class that submits and executes these echo messages. Executor is retrieved from `HazelcastInstance` and 1000 echo tasks are submitted.

```java
public class MasterMember {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    IExecutorService executor = hazelcastInstance.getExecutorService( "exec" );
    for ( int k = 1; k <= 1000; k++ ) {
      Thread.sleep( 1000 );
      System.out.println( "Producing echo task: " + k );
      executor.execute( new EchoTask( String.valueOf( k ) ) );
    }
    System.out.println( "EchoTaskMain finished!" );
  }
}
```

#### Executor Thread Configuration

By default, Executor is configured to have 8 threads in the pool. You can change that with the `pool-size` property in the declarative configuration (`hazelcast.xml`). An example is shown below (using the above Executor).

```xml
<executor-service name="exec">
  <pool-size>1</pool-size>
</executor-service>
```

<br></br>

***RELATED INFORMATION***


*Please refer to the [Executor Service Configuration section](#executor-service-configuration) for a full description of Hazelcast Distributed Executor Service configuration.*


#### Scaling


You can scale the Executor service both vertically (scale up) and horizontally (scale out).


To scale up, you should improve the processing capacity of the JVM. You can do this by increasing the `pool-size` property mentioned in the [Executor Thread Configuration section](#executor-thread-configuration) (i.e., increasing the thread count). However, please be aware of your JVM's capacity. If you think it cannot handle such an additional load caused by increasing the thread count, you may want to consider improving the JVM's resources (CPU, memory, etc.). As an example, set the `pool-size` to 5 and run the above `MasterMember`. You will see that `EchoTask` is run as soon as it is produced.


To scale out, more JVMs should be added instead of increasing only one JVM's capacity. In reality, you may want to expand your cluster by adding more physical or virtual machines. For example, in the EchoTask example in the [Runnable section](#runnable), you can create another Hazelcast instance. That instance will automatically get involved in the executions started in `MasterMember` and start processing.
