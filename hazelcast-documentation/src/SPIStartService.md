
### Start the Service

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

