## Installation

### Hazelcast

To download and install Hazelcast, you only need to:

-   Download `hazelcast-<`*version*`>.zip` from [www.hazelcast.org](http://www.hazelcast.org/download/).

-   Unzip `hazelcast-<`*version*`>.zip` file.

-   Add `hazelcast-<`*version*`>.jar` file into your classpath.


As an alternative to downloading and installing Hazelcast, you can find Hazelcast in standard Maven repositories. If your project uses Maven, you do not need to add additional repositories to your `pom.xml` or add `hazelcast-<`*version*`>.jar` file into your classpath (Maven does that for you). Just add the following lines to your `pom.xml`:

```xml
<dependencies>
	<dependency>
		<groupId>com.hazelcast</groupId>
		<artifactId>hazelcast</artifactId>
		<version>3.3</version>
	</dependency>
</dependencies>
```

