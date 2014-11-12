
## Security Interceptor

![](images/enterprise-onlycopy.jpg)

Hazelcast allows you to intercept every remote operation executed by the client. This lets you add a very flexible custom security logic. To do this, implement `com.hazelcast.security.SecurityInterceptor`.

```java
public class MySecurityInterceptor implements SecurityInterceptor {

  public void before( Credentials credentials, String serviceName,
                      String methodName, Parameters parameters )
      throws AccessControlException {
    // credentials: client credentials 
    // serviceName: MapService.SERVICE_NAME, QueueService.SERVICE_NAME, ... etc
    // methodName: put, get, offer, poll, ... etc
    // parameters: holds parameters of the executed method, iterable.
  }

  public void after( Credentials credentials, String serviceName,
                     String methodName, Parameters parameters ) {
    // can be used for logging etc.
  }
}
```

The `before` method will be called before processing the request on the remote server. The `after` method will be called after the processing. Exceptions thrown while executing the `before` method will propagate to the client, but exceptions thrown while executing the `after` method will be suppressed.  
