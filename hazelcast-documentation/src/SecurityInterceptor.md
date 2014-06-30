
## Security Interceptor

![](images/enterprise-onlycopy.jpg)

Hazelcast allows you to intercept every remote-operation executed via client. This provides ability to add very flexible custom security logic. You should implement `com.hazelcast.security.SecurityInterceptor`

```java
public class MySecurityInterceptor implements SecurityInterceptor {

    public void before(Credentials credentials, String serviceName, String methodName, Parameters parameters) throws AccessControlException {
        //credentials: client credentials 
        //serviceName: MapService.SERVICE_NAME, QueueService.SERVICE_NAME, ... etc
        //methodName: put, get, offer, poll, ... etc
        //parameters: holds parameters of the executed method, iterable.
    }

    public void after(Credentials credentials, String serviceName, String methodName, Parameters parameters) {
        // can be used for logging etc.
    }
}
```

`before` method will be called before processing the request on the remote server and `after` method will be called after the processing. Any exception thrown while executing `before` method will propagate to client but exceptions thrown while executing `after` method will be suppressed.  
