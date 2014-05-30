

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




