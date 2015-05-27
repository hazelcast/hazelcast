


### Execution Cancellation

A task in the code you execute in a cluster might take longer than expected. If you cannot stop/cancel that task, it will keep eating your resources. 

To cancel a task, you can use the standard Java executor framework's `cancel()` API. This framework encourages us to code and design for cancellations, a highly ignored part of software development.

#### Example Task to Cancel

The Fibonacci callable class below calculates the Fibonacci number for a given number. In the `calculate` method, we check if the current thread is interrupted so that the code can respond to cancellations once the execution is started. 

```java
public class Fibonacci<Long> implements Callable<Long>, Serializable {
  int input = 0; 

  public Fibonacci() { 
  } 

  public Fibonacci( int input ) { 
    this.input = input;
  } 

  public Long call() {
    return calculate( input );
  }

  private long calculate( int n ) {
    if ( Thread.currentThread().isInterrupted() ) {
      return 0;
    }
    if ( n <= 1 ) {
      return n;
    } else {
      return calculate( n - 1 ) + calculate( n - 2 );
    }
  }
}
```

#### Example Method to Execute and Cancel the Task

The `fib()` method below submits the Fibonacci calculation task above for number 'n' and waits a maximum of 3 seconds for the result. If the execution does not completed in 3 seconds, `future.get()` will throw a `TimeoutException` and upon catching it, we cancel the execution, saving some CPU cycles.

```java
long fib( int n ) throws Exception {
  HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
  IExecutorService es = hazelcastInstance.getExecutorService();
  Future future = es.submit( new Fibonacci( n ) );  
  try {
    return future.get( 3, TimeUnit.SECONDS );
  } catch ( TimeoutException e ) {
    future.cancel( true );            
  }
  return -1;
}
```

`fib(20)` will probably take less than 3 seconds. However, `fib(50)` will take much longer. (This is not an example for writing better Fibonacci calculation code, but for showing how to cancel a running execution that takes too long.) The method `future.cancel(false)` can only cancel execution before it is running (executing), but `future.cancel(true)` can interrupt running executions if your code is able to handle the interruption. If you are willing to cancel an already running task, then your task should be designed to handle interruptions. If the `calculate (int n)` method did not have the `(Thread.currentThread().isInterrupted())` line, then you would not be able to cancel the execution after it is started.


