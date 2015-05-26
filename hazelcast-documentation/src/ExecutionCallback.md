


### Execution Callback

You can use the `ExecutionCallback` offered by Hazelcast to asynchronously be notified when the execution is done. 

#### Example Task to Callback

Let's use the Fibonacci series to explain this. The example code below is the calculation that will be executed. Note that it is Callable and Serializable.

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
    if (n <= 1) {
      return n;
    } else {
      return calculate( n - 1 ) + calculate( n - 2 );
    }
  }
}
```

#### Example Method to Callback the Task

The example code below submits the Fibanacci calculation to `ExecutionCallback` and prints the result asynchronously. `ExecutionCallback` has the methods `onResponse` and `onFailure`. In this example code, `onResponse` is called upon a valid response and prints the calculation result, whereas `onFailure` is called upon a failure and prints the stacktrace.


```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.Future;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
IExecutorService es = hazelcastInstance.getExecutorService();
Callable<Long> task = new Fibonacci( 10 );

es.submit(task, new ExecutionCallback<Long> () {

  @Override
  public void onResponse( Long response ) {
    System.out.println( "Fibonacci calculation result = " + response );
  }

  @Override
  public void onFailure( Throwable t ) {
    t.printStackTrace();
  }
};
```


