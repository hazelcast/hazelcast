## Creating Cluster Groups

You can create cluster groups. To do this, use the `group` configuration element. 

By specifying a group name and group password, you can separate your clusters in a simple way. Example groupings can be by *development*, *production*, *test*, *app*, etc. The following is an example declarative configuration.

```xml
<hazelcast>
  <group>
    <name>app1</name>
    <password>app1-pass</password>
  </group>
  ...
</hazelcast>
```

You can define the cluster groups also using the programmatic configuration. A JVM can host multiple Hazelcast instances. Each Hazelcast instance can only participate in one group and it only joins to its own group, it does not mess with others. The following code example creates three separate Hazelcast instances: `h1` belongs to the `app1` cluster, while `h2` and `h3` belong to the `app2` cluster.

```java
Config configApp1 = new Config();
configApp1.getGroupConfig().setName( "app1" ).setPassword( "app1-pass" );

Config configApp2 = new Config();
configApp2.getGroupConfig().setName( "app2" ).setPassword( "app2-pass" );

HazelcastInstance h1 = Hazelcast.newHazelcastInstance( configApp1 );
HazelcastInstance h2 = Hazelcast.newHazelcastInstance( configApp2 );
HazelcastInstance h3 = Hazelcast.newHazelcastInstance( configApp2 );
```
