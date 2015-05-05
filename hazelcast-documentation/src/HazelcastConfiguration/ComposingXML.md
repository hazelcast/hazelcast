
## Composing Declarative Configuration

You can compose the declarative configuration of your Hazelcast or Hazelcast Client from multiple declarative configuration snippets. In order to compose a declarative configuration, you can use the `<import/>` element to load different declarative configuration files. Please see the following examples.

Let's say you want to compose the declarative configuration for Hazelcast out of two configurations: `development-group-config.xml` and `development-network-config.xml`. These two configurations are shown below.

`development-group-config.xml`:

```xml
<hazelcast>
  <group>
      <name>dev</name>
      <password>dev-pass</password>
  </group>
</hazelcast>
```
<br></br>

`development-network-config.xml`:

```xml
<hazelcast>
  <network>
    <port auto-increment="true" port-count="100">5701</port>
    <join>
        <multicast enabled="true">
            <multicast-group>224.2.2.3</multicast-group>
            <multicast-port>54327</multicast-port>
        </multicast>
    </join>
  </network>
</hazelcast>
```

To get your example Hazelcast declarative configuration out of the above two, use the `<import/>` element as shown below.


```xml
<hazelcast>
  <import resource="development-group-config.xml"/>
  <import resource="development-network-config.xml"/>
</hazelcast>
```

This feature also applies to the declarative configuration of Hazelcast Client. Please see the following examples.


`client-group-config.xml`:

```xml
<hazelcast-client>
  <group>
      <name>dev</name>
      <password>dev-pass</password>
  </group>
</hazelcast-client>
```
<br></br>

`client-network-config.xml`:

```xml
<hazelcast-client>
    <network>
        <cluster-members>
            <address>127.0.0.1:7000</address>
        </cluster-members>
    </network>
</hazelcast-client>
```

To get a Hazelcast Client declarative configuration from the above two examples, use the `<import/>` element as shown below.

```xml
<hazelcast-client>
  <import resource="client-group-config.xml"/>
  <import resource="client-network-config.xml"/>
</hazelcast>
```


<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *You can only use `<import/>` element on top level of the XML hierarchy.*
<br></br>

- XML resources can be loaded from classpath and filesystem. Please see the following example.

```xml
<hazelcast>
  <import resource="file:///etc/hazelcast/development-group-config.xml"/> <!-- loaded from filesystem -->
  <import resource="classpath:development-network-config.xml"/>  <!-- loaded from classpath -->
</hazelcast>
```

- You can use property placeholders in the `<import/>` elements. Please see the following example.

```xml
<hazelcast>
  <import resource="${environment}-group-config.xml"/>
  <import resource="${environment}-network-config.xml"/>
</hazelcast>
```
