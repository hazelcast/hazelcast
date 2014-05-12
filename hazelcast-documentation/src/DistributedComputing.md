
# Distributed Computing

## Executor Service

### Overview

One of the coolest features of Java 1.5 is the Executor framework, which allows you to asynchronously execute your tasks, logical units of works, such as database query, complex calculation, image rendering, etc.

The default implementation of this framework (ThreadPoolExecutor) is designed to run within a single JVM. In distributed systems, this implementation is not desired since you may want a task submitted in a JVM and processed in another one. Hazelcast offers IExecutorService to be used in distributed environments that implements `java.util.concurrent.ExecutorService`, to serve the applications requiring computational and data processing power.

With IExecutorService, you can execute tasks asynchronously and perform other useful things meanwhile. When ready, get the result and move on. If execution of the task takes longer than expected, you may consider canceling the task execution. In Java Executor framework, tasks are implemented as `java.util.concurrent.Callable` and `java.util.Runnable`. If you need to return a value and submit to Executor, Callable is used. Otherwise, Runnable is used (if you do not need to return a value). Naturally, tasks should be `Serializable` since they will be distributed.

#### Callable

Below is a sample Callable.

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
```

Echo callable above, for instance, in its `call()` method, is returning the local member and the input passed in. Remember that `instance.getCluster().getLocalMember()` returns the local member and `toString()` returns the member's address `(IP + port)` in String form, just to see which member actually executed the code for our example. Of course, `call()` method can do and return anything you like.

Executing a task by using executor framework is very straight forward. Simply obtain an `ExecutorService` instance, generally via `Executors` and submit the task which returns a `Future`. After executing task, you do not have to wait for execution to complete, you can process other things and when ready use the `future` object to retrieve the result as shown in code below.

```java
ExecutorService executorService = Executors.newSingleThreadExecutor();
Future<String> future = executorService.submit(new Echo("myinput"));
//while it is executing, do some useful stuff
//when ready, get the result of your execution
String result = future.get();
```

Please note that Echo callable in the very above sample also implements Serializable interface, since it may be sent to another JVM to be processed.


#### Runnable

Let's now go with a sample that is Runnable. Below is a task that waits for some time and echoes a message.

```java
public class EchoTask implements Runnable, Serializable {
    private final String msg;

    public EchoTask(String msg) {
    this.msg = msg;
}

    @Override
    public void run() {
       try {
         Thread.sleep(5000);
       } catch (InterruptedException e) {
       }
       System.out.println("echo:" + msg);
    }
}
```

Then let's write a class that submits and executes echo messages:

```java
public class MasterMember {
    public static void main(String[] args) throws Exception {
       HazelcastInstance hz = Hazelcast.newHazelcastInstance();
       IExecutorService executor = hz.getExecutorService("exec");
       for (int k = 1; k <= 1000; k++) {
          Thread.sleep(1000);
          System.out.println("Producing echo task: " + k);
          executor.execute(new EchoTask("" + k));
       }
    System.out.println("EchoTaskMain finished!");
   }
}
```

Above, Executor is retrieved from HazelcastInstance and 1000 echo tasks are submitted.

#### Thread Configuration

By default, Executor is configured to have 8 threads in the pool. It can be changed through the `pool-size` property using declarative configuration (`hazelcast.xml`). A sample is shown below (using above Executor).

```xml
<executor-service name="exec">
    <pool-size>1</pool-size>
</executor-service>
```

#### Scaling


Executor service can be scaled both vertically (scale up) and horizontally (scale out).


To scale up, processing capacity of the JVM should be improved. This can be done by increasing the `pool-size` property mentioned in the previous subsection (i.e., increasing the thread count). However, please be aware of your JVM's capacity. If you think it cannot handle such additional load caused by increasing the thread count, you may want to consider improving the JVM's resources (CPU, memory, etc.). As an example, please set the `pool-size` to 5 and run the above `MasterMember`. You will see that `EchoTask` is run as soon as it is produced.


To scale out, more JVMs should be added instead of increasing only one JVM's capacity. In reality, you may want to expand your cluster by adding more physical or virtual machines. For the EchoTask sample above, you can simply create another Hazelcast instance. It will automatically get involved in the executions started in `MasterMember` and start processing.
