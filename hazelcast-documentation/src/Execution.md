

### Execution

Distributed executor service is a distributed implementation of `java.util.concurrent.ExecutorService`. It allows you to execute your code in the cluster. In this section, all the code samples are based on the Echo class above. Please note that Echo class is `Serializable`. You can ask Hazelcast to execute your code (`Runnable, Callable`);

- on a specific cluster member you choose,
- on the member owning the key you choose,
- on the member Hazelcast will pick, and
- on all or subset of the cluster members.

```java
import com.hazelcast.core.Member;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;   
import java.util.Set;

public void echoOnTheMember( String input, Member member ) throws Exception {
  Callable<String> task = new Echo( input );
  HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
  IExecutorService executorService = 
      hazelcastInstance.getExecutorService( "default" );
      
  Future<String> future = executorService.submitToMember( task, member );
  String echoResult = future.get();
}

public void echoOnTheMemberOwningTheKey( String input, Object key ) throws Exception {
  Callable<String> task = new Echo( input );
  HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
  IExecutorService executorService =
      hazelcastInstance.getExecutorService( "default" );
      
  Future<String> future = executorService.submitToKeyOwner( task, key );
  String echoResult = future.get();
}

public void echoOnSomewhere( String input ) throws Exception { 
  HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
  IExecutorService executorService =
      hazelcastInstance.getExecutorService( "default" );
      
  Future<String> future = executorService.submit( new Echo( input ) );
  String echoResult = future.get();
}

public void echoOnMembers( String input, Set<Member> members ) throws Exception {
  HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
  IExecutorService executorService = 
      hazelcastInstance.getExecutorService( "default" );
      
  Map<Member, Future<String>> futures = executorService
      .submitToMembers( new Echo( input ), members );
      
  for ( Future<String> future : futures.values() ) {
    String echoResult = future.get();
    // ...
  }
}
```


![image](images/NoteSmall.jpg) ***NOTE:*** *You can obtain the set of cluster members via `HazelcastInstance#getCluster().getMembers()` call.*


