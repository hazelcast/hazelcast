


### Execution Cancellation

What if the code you execute in cluster takes longer than expected. If you cannot stop/cancel that task, it will keep eating your resources. Standard Java executor framework solves this problem with by introducing `cancel()` API and "encouraging" us to code and design for cancellations, which is highly ignored part of software development.

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

The callable class above calculates the Fibonacci number for a given number. In the `calculate` method, we are checking to see if the current thread is interrupted so that code can be responsive to cancellations once the execution is started. Below `fib()` method submits the Fibonacci calculation task for number 'n' and waits maximum 3 seconds for result. If the execution does not completed in 3 seconds, `future.get()` will throw `TimeoutException` and upon catching, it we interruptibly cancel the execution for saving some CPU cycles.

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

`fib(20)` will probably take less than 3 seconds but, `fib(50)` will take way longer. (This is not the example for writing better Fibonacci calculation code, but for showing how to cancel a running execution that takes too long). The method `future.cancel(false)` can only cancel execution before it is running (executing), but `future.cancel(true)` can interrupt running executions if your code is able to handle the interruption. So, if you are willing to be able to cancel already running task, then your task has to be designed to handle interruption. If `calculate (int n)` method did not have `(Thread.currentThread().isInterrupted())` line, then you would not be able to cancel the execution after it is started.


