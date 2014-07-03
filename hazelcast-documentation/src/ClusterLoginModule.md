

## ClusterLoginModule

![](images/enterprise-onlycopy.jpg)


All security attributes are carried in `Credentials` object and `Credentials` is used by [LoginModule](http://docs.oracle.com/javase/7/docs/api/javax/security/auth/spi/LoginModule.html)s during authentication process. Accessing user supplied attributes from `LoginModule`s is done by [CallbackHandler](http://docs.oracle.com/javase/7/docs/api/javax/security/auth/callback/CallbackHandler.html)s. To provide access to Credentials object, Hazelcast uses its own specialized `CallbackHandler`. During initialization of `LoginModules` Hazelcast will pass this special `CallbackHandler` into `LoginModule.initialize()` method.

LoginModule implementations should create an instance of `com.hazelcast.security.CredentialsCallback` and call `handle(Callback[] callbacks)` method of `CallbackHandler` during login process. `CredentialsCallback.getCredentials()` will return the supplied `Credentials` object.

```java
public class CustomLoginModule implements LoginModule {
    CallbackHandler callbackHandler;
    Subject subject;
    
    public final void initialize(Subject subject, CallbackHandler callbackHandler,
        Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
    }

    public final boolean login() throws LoginException {
        CredentialsCallback callback = new CredentialsCallback();
        try {
            callbackHandler.handle(new Callback[]{callback});
            credentials = cb.getCredentials();
        } catch (Exception e) {
            throw new LoginException(e.getMessage());
        }
        ...
    }
...
}
```

To use default Hazelcast permission policy, an instance of `com.hazelcast.security.ClusterPrincipal` that holding `Credentials` object must be created and added to `Subject.principals onLoginModule.commit()` as shown below.

```java
public class MyCustomLoginModule implements LoginModule {
...
    public boolean commit() throws LoginException {
        ...
        final Principal principal = new ClusterPrincipal(credentials);
        subject.getPrincipals().add(principal);
        
        return true;
    }
    ...
}
```

Hazelcast also has an abstract implementation of `LoginModule` that does callback and cleanup operations and holds resulting `Credentials` instance. `LoginModule`s extending `ClusterLoginModule` can access `Credentials`, `Subject`, `LoginModule` instances and options and `sharedState` maps. Extending `ClusterLoginModule` is recommended instead of implementing all required stuff.

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

***RELATED INFORMATION***

*Please refer to [JAAS Reference Guide](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) for further information.*