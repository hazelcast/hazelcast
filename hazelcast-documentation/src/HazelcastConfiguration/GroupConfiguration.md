
## Group Configuration

This configuration is used to create multiple Hazelcast clusters. The cluster members (nodes) and clients having the same group configuration (i.e. same group name and password) forms a private cluster. 

Each cluster will have its own group and it will not interfere with other clusters. The name of the element to configure cluster groups is `group`.

The following are example configurations.

**Declarative:**

```xml
<group>
	<name>testCluster</name>
	<password>1q2w3e</password>
<group>
```

**Programmatic:**

```java
GroupConfig groupConfig = new GroupConfig();
groupConfig.setName( "testCluster" ).setPassword( "1q2w3e" );
```
   
It has below elements.

- `name`: Name of the group to be created.
- `password`: Password of the group to be created.


