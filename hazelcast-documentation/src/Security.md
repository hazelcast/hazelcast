

# Security



## Socket Interceptor

![](images/enterprise-onlycopy.jpg)


Hazelcast allows you to intercept socket connections before a node joins to cluster or a client connects to a node. This provides ability to add custom hooks to join/connection procedure (like identity checking using Kerberos, etc.). You should implement `com.hazelcast.nio.MemberSocketInterceptor` for members and `com.hazelcast.nio.SocketInterceptor` for clients.

```java
public class MySocketInterceptor implements MemberSocketInterceptor {
  public void init( SocketInterceptorConfig socketInterceptorConfig ) {
    // initialize interceptor
  }

  void onConnect( Socket connectedSocket ) throws IOException {
    // do something meaningful when connected
  }

  public void onAccept( Socket acceptedSocket ) throws IOException {
    // do something meaningful when accepted a connection
  }
}
```

```xml
<hazelcast>
  ...
  <network>
    ...
    <socket-interceptor enabled="true">
      <class-name>com.hazelcast.examples.MySocketInterceptor</class-name>
      <properties>
        <property name="kerberos-host">kerb-host-name</property>
        <property name="kerberos-config-file">kerb.conf</property>
      </properties>
    </socket-interceptor>
  </network>
  ...
</hazelcast>
```

```java
public class MyClientSocketInterceptor implements SocketInterceptor {
  void onConnect( Socket connectedSocket ) throws IOException {
    // do something meaningful when connected
  }
}

ClientConfig clientConfig = new ClientConfig();
clientConfig.setGroupConfig( new GroupConfig( "dev", "dev-pass" ) )
    .addAddress( "10.10.3.4" );

MyClientSocketInterceptor clientSocketInterceptor = new MyClientSocketInterceptor();
clientConfig.setSocketInterceptor( clientSocketInterceptor );
HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );
```


