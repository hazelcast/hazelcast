

## Cluster Utilities


### Cluster Interface

Hazelcast allows you to register for membership events so you will be notified when members are added or removed. You can also get the set of cluster members.

```java
import com.hazelcast.core.*;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Cluster cluster = hazelcastInstance.getCluster();
cluster.addMembershipListener( new MembershipListener() {
  public void memberAdded( MembershipEvent membershipEvent ) {
    System.out.println( "MemberAdded " + membershipEvent );
  }

  public void memberRemoved( MembershipEvent membershipEvent ) {
    System.out.println( "MemberRemoved " + membershipEvent );
  }
} );

Member localMember  = cluster.getLocalMember();
System.out.println ( "my inetAddress= " + localMember.getInetAddress() );

Set setMembers  = cluster.getMembers();
for ( Member member : setMembers ) {
  System.out.println( "isLocalMember " + member.localMember() );
  System.out.println( "member.inetaddress " + member.getInetAddress() );
  System.out.println( "member.port " + member.getPort() );
}
```

