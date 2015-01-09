

## ClusterLoginModule

![](images/enterprise-onlycopy.jpg)


All security attributes are carried in the `Credentials` object. `Credentials` is used by [LoginModule](http://docs.oracle.com/javase/7/docs/api/javax/security/auth/spi/LoginModule.html)s during the authentication process. User supplied attributes from `LoginModule`s are accessed by [CallbackHandler](http://docs.oracle.com/javase/7/docs/api/javax/security/auth/callback/CallbackHandler.html)s. To access the `Credentials` object, Hazelcast uses its own specialized `CallbackHandler`. During initialization of `LoginModules`, Hazelcast passes this special `CallbackHandler` into the `LoginModule.initialize()` method.

`LoginModule` implementations should create an instance of `com.hazelcast.security.CredentialsCallback` and call the `handle(Callback[] callbacks)` method of `CallbackHandler` during the login process. 

`CredentialsCallback.getCredentials()` returns the supplied `Credentials` object.

```java
public class CustomLoginModule implements LoginModule {
  CallbackHandler callbackHandler;
  Subject subject;
    
  public void initialize( Subject subject, CallbackHandler callbackHandler,
                          Map<String, ?> sharedState, Map<String, ?> options ) {
    this.subject = subject;
    this.callbackHandler = callbackHandler;
  }

  public final boolean login() throws LoginException {
    CredentialsCallback callback = new CredentialsCallback();
    try {
      callbackHandler.handle( new Callback[] { callback } );
      credentials = cb.getCredentials();
    } catch ( Exception e ) {
      throw new LoginException( e.getMessage() );
    }
    ...
  }
  ...
}
```

To use the default Hazelcast permission policy, you must create an instance of `com.hazelcast.security.ClusterPrincipal` that holds the `Credentials` object, and you must add it to `Subject.principals onLoginModule.commit()` as shown below.

```java
public class MyCustomLoginModule implements LoginModule {
  ...
  public boolean commit() throws LoginException {
    ...
    Principal principal = new ClusterPrincipal( credentials );
    subject.getPrincipals().add( principal );
        
    return true;
  }
  ...
}
```

Hazelcast has an abstract implementation of `LoginModule` that does callback and cleanup operations and holds the resulting `Credentials` instance. `LoginModule`s extending `ClusterLoginModule` can access `Credentials`, `Subject`, `LoginModule` instances and options, and `sharedState` maps. Extending the `ClusterLoginModule` is recommended instead of implementing all required stuff.

```java
package com.hazelcast.security;
...
public abstract class ClusterLoginModule implements LoginModule {

  protected abstract boolean onLogin() throws LoginException;
  protected abstract boolean onCommit() throws LoginException;
  protected abstract boolean onAbort() throws LoginException;
  protected abstract boolean onLogout() throws LoginException;
}
```
<br></br>

## Enterprise Integration

Using the above API it should be possible to implement a `LoginModule` that performs authentication against the Security System of your choice, possibly an LDAP store like [Apache Directory](https://directory.apache.org/) or some other corporate standard you have.  For example you may wish to have your clients send an identification token in the `Credentials` object.  This token can then be sent to your back-end security system via the `LoginModule` that runs on the cluster side.

Additionally the same system may authenticate the user and also then return the roles that are attributed to the user.  These roles can then be used for data structure authorization. 

***RELATED INFORMATION***

*Please refer to [JAAS Reference Guide](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) for further information.*