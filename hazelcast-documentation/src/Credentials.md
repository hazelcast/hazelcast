

## Credentials

![](images/enterprise-onlycopy.jpg)


One of the key elements in Hazelcast security is `Credentials` object. It is used to carry all credentials of an endpoint (member or client). Credentials is an interface which extends `Serializable` and has three methods to be implemented. The users can either implement `Credentials` interface or extend `AbstractCredentials` class, which is an abstract implementation of `Credentials`, according to their needs.

```java
package com.hazelcast.security;
public interface Credentials extends Serializable {
  String getEndpoint();
  void setEndpoint( String endpoint ) ;    
  String getPrincipal() ;    
}
```

`Credentials.setEndpoint()` method is called by Hazelcast when authentication request arrives to node before authentication takes place.

```java
package com.hazelcast.security;
...
public abstract class AbstractCredentials implements Credentials, DataSerializable {
  private transient String endpoint;
  private String principal;
  ...
}
```

`UsernamePasswordCredentials`, a custom implementation of Credentials can be found in Hazelcast `com.hazelcast.security` package. It is used by default configuration during authentication process of both members and clients.

```java
package com.hazelcast.security;
...
public class UsernamePasswordCredentials extends Credentials {
  private byte[] password;
  ...
}
```
