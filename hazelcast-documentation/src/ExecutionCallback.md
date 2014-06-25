


### Execution Callback

`ExecutionCallback` offered by Hazelcast allows you to asynchronously get notified when the execution is done. 

Let's use the Fibonacci series to explain this. Below sample code is the calculation.

```java
public class Fibonacci<Long> implements Callable<Long>, Serializable {
    int input = 0;

    public Fibonacci() {
    }

    public Fibonacci(int input) {
        this.input = input;
    }

    public Long call() {
        return calculate (input);
    }

    private long calculate (int n) {
        if (n <= 1) return n;
        else return calculate(n-1) + calculate(n-2);
    }
}
```

And, below sample code prints the result asynchronously.


```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.Future;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
IExecutorService es = hz.getExecutorService();
Callable<Long> task = new Fibonacci(10);

es.submit(task, new ExecutionCallback<Long> () {

    public void onResponse(Long response) {
        System.out.println("Fibonacci calculation result = " + response);
    }

    public void onFailure(Throwable t) {
        t.printStackTrace();
    }

};
```

As it can be seen, `ExecutionCallback` has the methods `onResponse` and `onFailure`. The former one in the above code is called upon a valid response and prints the calculation result, whereas the latter one is called upon a failure and prints the stacktrace.

