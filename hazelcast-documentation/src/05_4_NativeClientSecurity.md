

## Native Client Security

Hazelcast's Client security includes both authentication and authorization.

### Authentication

Authentication mechanism just works the same as cluster member authentication. Implementation of client authentication requires a Credentials and one or more LoginModule(s). Client side does not have/need a factory object to create Credentials objects like `ICredentialsFactory`. Credentials must be created at client side and sent to connected node during connection process.

```xml
<security enabled="true">
    <client-login-modules>
        <login-module class-name="com.hazelcast.examples.MyRequiredClientLoginModule" usage="required">
            <properties>
                <property name="property3">value3</property>
            </properties>
        </login-module>
        <login-module class-name="com.hazelcast.examples.MySufficientClientLoginModule" usage="sufficient">
            <properties>
                <property name="property4">value4</property>
            </properties>
        </login-module>
        <login-module class-name="com.hazelcast.examples.MyOptionalClientLoginModule" usage="optional">
            <properties>
                <property name="property5">value5</property>
            </properties>
        </login-module>
    </client-login-modules>
    ...
</security>
```

You can define as many as `LoginModules` you wanted in configuration. Those are executed in given order. Usage attribute has 4 values; 'required', 'requisite', 'sufficient' and 'optional' as defined in `javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag`.

```java
final Credentials credentials = new UsernamePasswordCredentials("dev", "dev-pass");
HazelcastInstance client = HazelcastClient.newHazelcastClient(credentials, "localhost");
```

### Authorization

Hazelcast client authorization is configured by a client permission policy. Hazelcast has a default permission policy implementation that uses permission configurations defined in Hazelcast security configuration. Default policy permission checks are done against instance types (map, queue...), instance names (map, queue etc. name), instance actions (put, read, remove, add...), client endpoint addresses and client principal defined by Credentials object. Instance and principal names and endpoint addresses can be defined as wildcards(*). Take a look at Wildcard Name Configuration and Network Configuration pages.

```xml
<security enabled="true">
    <client-permissions>
        <!-- Principal 'admin' from endpoint '127.0.0.1' has all permissions. -->
        <all-permissions principal="admin">
            <endpoints>
                <endpoint>127.0.0.1</endpoint>
            </endpoints>
        </all-permissions>
        
        <!-- Principals named 'dev' from all endpoints have 'create', 'destroy', 
            'put', 'read' permissions for map named 'default'. -->
        <map-permission name="default" principal="dev">
            <actions>
                <action>create</action>
                <action>destroy</action>
                <action>put</action>
                <action>read</action>
            </actions>
        </map-permission>
        
        <!-- All principals from endpoints '127.0.0.1' or matching to '10.10.*.*' 
            have 'put', 'read', 'remove' permissions for map
            whose name matches to 'com.foo.entity.*'. -->
        <map-permission name="com.foo.entity.*">
            <endpoints>
                <endpoint>10.10.*.*</endpoint>
                <endpoint>127.0.0.1</endpoint>
            </endpoints>
            <actions>
                <action>put</action>
                <action>read</action>
                <action>remove</action>
            </actions>
        </map-permission>
        
        <!-- Principals named 'dev' from endpoints matching to either 
            '192.168.1.1-100' or '192.168.2.*' 
            have 'create', 'add', 'remove' permissions for all queues. -->
        <queue-permission name="*" principal="dev">
            <endpoints>
                <endpoint>192.168.1.1-100</endpoint>
                <endpoint>192.168.2.*</endpoint>
            </endpoints>
            <actions>
                <action>create</action>
                <action>add</action>
                <action>remove</action>
            </actions>
        </queue-permission>
        
        <!-- All principals from all endpoints have transaction permission.-->
        <transaction-permission />
    </client-permissions>
</security>
```

Users also can define their own policy by implementing `com.hazelcast.security.IPermissionPolicy`.

```java
package com.hazelcast.security;
/**
 * IPermissionPolicy is used to determine any Subject's 
 * permissions to perform a security sensitive Hazelcast operation.
 *
 */
public interface IPermissionPolicy {
    void configure(SecurityConfig securityConfig, Properties properties);
    
    PermissionCollection getPermissions(Subject subject, Class<? extends Permission> type);
    
    void destroy();
}
```

Permission policy implementations can access client-permissions in configuration by using `SecurityConfig.getClientPermissionConfigs()` during `configure(SecurityConfig securityConfig, Properties properties)` method is called by Hazelcast. `IPermissionPolicy.getPermissions(Subject subject, Class<? extends Permission> type)` method is used to determine a client request has been granted permission to do a security-sensitive operation. Permission policy should return a `PermissionCollection` containing permissions of given type for given `Subject`. Hazelcast access controller will call `PermissionCollection.implies(Permission)` on returning `PermissionCollection` and will decide if current `Subject` has permitted to access to requested resources or not.

### Permissions

- All Permission

```xml
<all-permissions principal="principal">
    <endpoints>
        ...
    </endpoints>
</all-permissions>
```

- Map Permission

```xml
<map-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</map-permission>
```
Actions: all, create, destroy, put, read, remove, lock, intercept, index, listen

- Queue Permission

```xml
<queue-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</queue-permission>
```

Actions: all, create, destroy, add, remove, read, listen

- Multimap Permission

```xml
<multimap-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</multimap-permission>
```
Actions: all, create, destroy, put, read, remove, listen, lock

- Topic Permission

```xml
<topic-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</topic-permission>
```
Actions: create, destroy, publish, listen

- List Permission

```xml
<list-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</list-permission>
```
Actions: all, create, destroy, add, read, remove, listen

- Set Permission

```xml
<set-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</set-permission>
```
Actions: all, create, destroy, add, read, remove, listen

- Lock Permission

```xml
<lock-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</lock-permission>
```
Actions: all, create, destroy, lock, read

- AtomicLong Permission

```xml
<atomic-long-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</atomic-long-permission>
```
Actions: all, create, destroy, read, modify

- CountDownLatch Permission

```xml
<countdown-latch-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</countdown-latch-permission>
```
Actions: all, create, destroy, modify, read

- Semaphore Permission

```xml
<semaphore-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</semaphore-permission>
```
Actions: all, create, destroy, acquire, release, read

- Executor Service Permission

```xml
<executor-service-permission name="name" principal="principal">
    <endpoints>
        ...
    </endpoints>
    <actions>
        ...
    </actions>
</executor-service-permission>
```
Actions: all, create, destroy

- Transaction Permission

```xml
<transaction-permission principal="principal">
    <endpoints>
        ...
    </endpoints>
</transaction-permission>
```

