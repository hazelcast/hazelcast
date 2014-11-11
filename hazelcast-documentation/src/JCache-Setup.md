

## Setup and Configuration

This sub-chapter shows what is necessary to provide the JCache API and the Hazelcast JCache implementation for your application. In
addition, it demonstrates the different configuration options as well as a description of the configuration properties.

### Application Setup

To provide your application with this JCache functionality, your application needs the JCache API inside its classpath. This API is the bridge between the specified JCache standard and the implementation provided by Hazelcast.

The way to integrate the JCache API JAR into the application classpath depends on the build system used. For Maven, Gradle, SBT,
Ivy and many other build systems, all using Maven based dependency repositories, perform the integration by adding
the Maven coordinates to the build descriptor.

As already mentioned, next to the default Hazelcast coordinates that might be already part of the application, you have to add JCache
coordinates.

For Maven users, the coordinates look like the following code:

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```
With other build systems, you might need to describe the coordinates in a different way.

#### Activating Hazelcast as JCache Provider

To activate Hazelcast as the JCache provider implementation, add either `hazelcast-all.jar` or
`hazelcast.jar` to the classpath (if not already available) by either one of the following Maven snippets.

If you use `hazelcast-all.jar`:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-all</artifactId>
  <version>3.4</version>
</dependency>
```

If you use `hazelcast.jar`:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4</version>
</dependency>
```
The users of other build systems have to adjust the way of
defining the dependency to their needs.

#### Connecting Clients to Remote Server

When the users want to use Hazelcast clients to connect to a remote cluster, the `hazelcast-client.jar` dependency is also required
on the client side applications. This JAR is already included in `hazelcast-all.jar`. Or, you can add it to the classpath using the following
Maven snippet:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4</version>
</dependency>
```

For other build systems, e.g. ANT, the users have to download these dependencies from either the JSR-107 specification and
Hazelcast community website ([http://www.hazelcast.org](http://www.hazelcast.org)) or from the Maven repository search page
([http://search.maven.org](http://search.maven.org)).

