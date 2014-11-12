

## Credentials

![](images/enterprise-onlycopy.jpg)


One of the key elements in Hazelcast security is the `Credentials` object, which is used to carry all credentials of an endpoint (member or client). Credentials is an interface which extends `Serializable` and has three methods to implement. You can either implement the `Credentials` interface or extend the `AbstractCredentials` class, which is an abstract implementation of `Credentials`.

```java
package com.hazelcast.security;
public interface Credentials extends Serializable {
  String getEndpoint();
  void setEndpoint( String endpoint ) ;    
  String getPrincipal() ;    
}
```

Hazelcst calls the `Credentials.setEndpoint()` method when an authentication request arrives at the node before authentication takes place.

```java
package com.hazelcast.security;
...
public abstract class AbstractCredentials implements Credentials, DataSerializable {
  private transient String endpoint;
  private String principal;
  ...
}
```

`UsernamePasswordCredentials`, a custom implementation of Credentials, is in the Hazelcast `com.hazelcast.security` package. `UsernamePasswordCredentials` is used for default configuration during the authentication process of both members and clients.

```java
package com.hazelcast.security;
...
public class UsernamePasswordCredentials extends Credentials {
  private byte[] password;
  ...
}
```
