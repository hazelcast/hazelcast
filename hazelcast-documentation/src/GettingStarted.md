## Getting Started

### Installing Hazelcast

It is more than simple to start enjoying Hazelcast:

-   Download `hazelcast-<version>.zip` from [www.hazelcast.org](http://www.hazelcast.org/download/).

-   Unzip `hazelcast-<version>.zip` file.

-   Add `hazelcast-<version>.jar` file into your classpath.

That is all.

Alternatively, Hazelcast can be found in standard Maven repositories. So, if your project uses Maven, you do not need to add additional repositories to your `pom.xml`. Just add the following lines to `pom.xml`:

```xml
<dependencies>
	<dependency>
		<groupId>com.hazelcast</groupId>
		<artifactId>hazelcast</artifactId>
		<version>3.3</version>
	</dependency>
</dependencies>
```

