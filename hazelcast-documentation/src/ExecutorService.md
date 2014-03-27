

# Distributed Executor Service

One of the coolest features of Java 1.5 is the Executor framework, which allows you to asynchronously execute your tasks, logical units of works, such as database query, complex calculation, image rendering, etc. So, one nice way of executing such tasks would be running them asynchronously and doing other things meanwhile. When ready, get the result and move on. If execution of the task takes longer than expected, you may consider canceling the task execution. In Java Executor framework, tasks are implemented as `java.util.concurrent.Callable` and `java.util.Runnable`.

```java
import java.util.concurrent.Callable;
import java.io.Serializable;
import com.hazelcast.core.HazelcastInstanceAware;

public class Echo implements Callable<String>, Serializable, HazelcastInstanceAware {        transient HazelcastInstance instance;        String input = null;        public Echo() {        }        public Echo(String input) {            this.input = input;        }        public String call() {            return instance.getCluster().getLocalMember().toString() + ":" + input;        }        @Override        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {            instance = hazelcastInstance;        }    }```


Echo callable above, for instance, in its `call()` method, is returning the local member and the input passed in. 

Normally, `Echo` is sent from a node to another node. It is serialized, re-created and executed there. `HazelcastInstanceAware` implemented in the above sample states that, there is an instance on the destination node that will be injected into this class using the method `setHazelcastInstance`. Meaning that, when `HazelcastInstanceAware` is implemented, executor service injects an existing instance while it executes a runnable or callable.

Remember that `instance.getCluster().getLocalMember()` returns the local member and `toString()` returns the member's address `(IP + port)` in String form, just to see which member actually executed the code for our example. Of course, `call()` method can do and return anything you like. Executing a task by using executor framework is very straight forward. Simply obtain a `ExecutorService` instance, generally via `Executors` and submit the task which returns a `Future`. After executing task, you do not have to wait for execution to complete, you can process other things and when ready use the future object to retrieve the result as show in code below.

```java
ExecutorService executorService = Executors.newSingleThreadExecutor();
Future<String> future = executorService.submit (new Echo("myinput"));
//while it is executing, do some useful stuff
//when ready, get the result of your execution
String result = future.get();
```