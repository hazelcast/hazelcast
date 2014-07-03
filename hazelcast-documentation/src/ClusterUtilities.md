

## Cluster Utilities


### Cluster Interface

Hazelcast allows you to register for membership events to get notified when members added or removed. You can also get the set of cluster members.

```java
import com.hazelcast.core.*;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Cluster cluster = hazelcastInstance.getCluster();
cluster.addMembershipListener( new MembershipListener() {
  public void memberAdded( MembershipEvent membersipEvent ) {
    System.out.println( "MemberAdded " + membersipEvent );
  }

  public void memberRemoved( MembershipEvent membersipEvent ) {
    System.out.println( "MemberRemoved " + membersipEvent );
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


### Member Attributes
You can define various member attribues on your Hazelcast members. You can use member attributes to tag your members as your business logic requirements.

In order to define member attribute on a member you can either

- Provide `MemberAttributeConfig` to your `Config` object.

- Provide member attributes at runtime via attribute setter methods on `Member` interface.

For example, you can tag your members with their CPU characteristics and you can route CPU intensive tasks to those CPU-rich members.

```java
MemberAttributeConfig fourCore = new MemberAttributeConfig();
memberAttributeConfig.setIntAttribute( "CPU_CORE_COUNT", 4 );
MemberAttributeConfig twelveCore = new MemberAttributeConfig();
memberAttributeConfig.setIntAttribute( "CPU_CORE_COUNT", 12 );
MemberAttributeConfig twentyFourCore = new MemberAttributeConfig();
memberAttributeConfig.setIntAttribute( "CPU_CORE_COUNT", 24 );

Config member1Config = new Config();
config.setMemberAttributeConfig( fourCore );
Config member2Config = new Config();
config.setMemberAttributeConfig( twelveCore );
Config member3Config = new Config();
config.setMemberAttributeConfig( twentyFourCore );

HazelcastInstance member1 = Hazelcast.newHazelcastInstance( member1Config );
HazelcastInstance member2 = Hazelcast.newHazelcastInstance( member2Config );
HazelcastInstance member3 = Hazelcast.newHazelcastInstance( member3Config );

IExecutorService executorService = member1.getExecutorService( "processor" );

executorService.execute( new CPUIntensiveTask(), new MemberSelector() {
  @Override
  public boolean select(Member member) {
    int coreCount = (int) member.getIntAttribute( "CPU_CORE_COUNT" );
    // Task will be executed at either member2 or member3
    if ( coreCount > 8 ) { 
      return true;
    }
    return false;
  }
} );

HazelcastInstance member4 = Hazelcast.newHazelcastInstance();
// We can also set member attributes at runtime.
member4.setIntAttribute( "CPU_CORE_COUNT", 2 );
```
