
## Composing XML Configuration

You can compose your Hazelcast XML or Hazelcast-Client XML Configuration file from multiple XML configuration snippets. In order to compose XML configuration, you can use the `<import/>` element to load different XML configuration files. Please see the following examples.   

#### Example of Hazelcast XML Composing
`hazelcast-config.xml`:

```xml
<hazelcast>
  <import resource="development-group-config.xml"/>
  <import resource="development-network-config.xml"/>
</hazelcast>
```

`development-group-config.xml`:

```xml
<hazelcast>
  <group>
      <name>dev</name>
      <password>dev-pass</password>
  </group>
</hazelcast>
```

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
#### Example of Hazelcast Client XML Composing

`hazelcast-client-config.xml`:

```xml
<hazelcast-client>
  <import resource="client-group-config.xml"/>
  <import resource="client-network-config.xml"/>
</hazelcast>
```

`development-group-config.xml`:

```xml
<hazelcast-client>
  <group>
      <name>dev</name>
      <password>dev-pass</password>
  </group>
</hazelcast-client>
```

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

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *You can only use `<import/>` element on top level of the XML hierarchy.*
<br></br>

- XML resources can be loaded from classpath and filesystem. For example:

```xml
<hazelcast>
  <import resource="file:///etc/hazelcast/development-group-config.xml"/> <!-- loaded from filesystem -->
  <import resource="classpath:development-network-config.xml"/>  <!-- loaded from classpath -->
</hazelcast>
```

- You can use property placeholders in the `<import/>` elements. For example:

```xml
<hazelcast>
  <import resource="${environment}-group-config.xml"/>
  <import resource="${environment}-network-config.xml"/>
</hazelcast>
```
