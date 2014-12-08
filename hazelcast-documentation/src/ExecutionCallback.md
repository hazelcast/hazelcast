


### Execution Callback

`ExecutionCallback` offered by Hazelcast allows you to asynchronously be notified when the execution is done. 

Let's use the Fibonacci series to explain this. The example code below is the calculation.

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

The example code below prints the result asynchronously.


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

`ExecutionCallback` has the methods `onResponse` and `onFailure`. In the above code, `onResponse` is called upon a valid response and prints the calculation result, whereas `onFailure` is called upon a failure and prints the stacktrace.

