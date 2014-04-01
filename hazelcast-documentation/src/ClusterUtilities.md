

## Cluster Utilities



### Cluster Interface

Hazelcast allows you to register for membership events to get notified when members added or removed. You can also get the set of cluster members.

```java
import com.hazelcast.core.*;
import com.hazelcast.config.Config;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
Cluster cluster = hz.getCluster();
cluster.addMembershipListener(new MembershipListener(){
    public void memberAdded(MembershipEvent membersipEvent) {
        System.out.println("MemberAdded " + membersipEvent);
    }

    public void memberRemoved(MembershipEvent membersipEvent) {
        System.out.println("MemberRemoved " + membersipEvent);
    }
});

Member localMember  = cluster.getLocalMember();
System.out.println ("my inetAddress= " + localMember.getInetAddress());

Set setMembers  = cluster.getMembers();
for (Member member : setMembers) {
    System.out.println ("isLocalMember " + member.localMember());
    System.out.println ("member.inetaddress " + member.getInetAddress());
    System.out.println ("member.port " + member.getPort());
}
```


### Cluster Wide ID Generator

Hazelcast `IdGenerator` creates cluster wide unique IDs. Generated IDs are long type primitive values between 0 and `Long.MAX_VALUE`. ID generation occurs almost at the speed of `AtomicLong.incrementAndGet()`. Generated IDs are unique during the life cycle of the cluster. If the entire cluster is restarted, IDs start from 0 again or you can initialize to a value.

```java
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Hazelcast;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
IdGenerator idGenerator = hz.getIdGenerator("customer-ids");
idGenerator.init(123L); //Optional
long id = idGenerator.newId();
```


