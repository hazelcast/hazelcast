
## Group Configuration

This configuration is used to create multiple Hazelcast clusters. Each cluster will have its own group and it will not interfere with other clusters. Sample configurations are shown below. The name of the element to configure cluster groups is `group`.

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
   
It has below sub-elements.

- `name`: Name of the group to be created.
- `password`: Password of the group to be created.


