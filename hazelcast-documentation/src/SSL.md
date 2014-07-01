
## SSL

![](images/enterprise-onlycopy.jpg)

Hazelcast allows you to use SSL socket communication among all Hazelcast members. You need to implement `com.hazelcast.nio.ssl.SSLContextFactory` and configure SSL section in network configuration.

```java
public class MySSLContextFactory implements SSLContextFactory {
    public void init(Properties properties) throws Exception {
    }

    public SSLContext getSSLContext() {
        ...
        SSLContext sslCtx = SSLContext.getInstance(protocol);
        return sslCtx;
    }
}
```

```xml
<hazelcast>
    ...
    <network>
        ...
        <ssl enabled="true">
            <factory-class-name>com.hazelcast.examples.MySSLContextFactory</factory-class-name>
            <properties>
                <property name="foo">bar</property>
            </properties>
        </ssl>
    </network>
    ...
</hazelcast>
```

Hazelcast provides a default SSLContextFactory; `com.hazelcast.nio.ssl.BasicSSLContextFactory` which uses configured keystore to initialize `SSLContext`. Just define `keyStore` and `keyStorePassword`, and also you can set `keyManagerAlgorithm` (default `SunX509`), `trustManagerAlgorithm` (default `SunX509`) and `protocol` (default `TLS`).

```xml
<hazelcast>
    ...
    <network>
        ...
        <ssl enabled="true">
            <factory-class-name>com.hazelcast.nio.ssl.BasicSSLContextFactory</factory-class-name>
            <properties>
                <property name="keyStore">keyStore</property>
                <property name="keyStorePassword">keyStorePassword</property>
                <property name="keyManagerAlgorithm">SunX509</property>
                <property name="trustManagerAlgorithm">SunX509</property>
                <property name="protocol">TLS</property>
            </properties>
        </ssl>
    </network>
    ...
</hazelcast>
```

Hazelcast client has SSL support too. Client SSL configuration can be defined using programmatic configuration as shown below.

````java
Properties props = new Properties();
...
ClientConfig config = new ClientConfig();
config.getSocketOptions().setSocketFactory(new SSLSocketFactory(props));
```

You can also set `keyStore` and `keyStorePassword` through `javax.net.ssl.keyStore` and `javax.net.ssl.keyStorePassword` system properties. 

<font color='red'>***Note***:</font> *You cannot use SSL when [Hazelcast Encryption](#encryption) is enabled.*
