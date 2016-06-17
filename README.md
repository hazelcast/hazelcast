## Hazelcast Jet

[Hazelcast Jet](http://jet.hazelcast.org) is a distributed data processing engine built on top of Hazelcast.

### Features:

* Low latency and distributed general data processing framework with high throughput.
* Highly parallel and distributed stream and batch processing of data.
* Distributed java.util.stream API support for Hazelcast data structures such as IMap and IList.

### Code Samples

See [Hazelcast Jet Code Samples](https://github.com/hazelcast/hazelcast-jet-code-samples) for some examples.

### Documentation

See documentation at [jet.hazelcast.org](http://jet.hazelcast.org/)

### Requirements

Jet 0.1 is compatible with Hazelcast 3.7.x, and will work with both clients and members.

### Releases

Use Maven snippet:
```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>${hazelcast.jet.version}</version>
</dependency>
<repository>
    <id>cloudbees-release-repository</id>
    <url>https://repository-hazelcast-l337.forge.cloudbees.com/release/</url>
</repository>
```

### Snapshot Releases

Maven snippet:
```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>${hazelcast.jet.version}</version>
</dependency>
<repository>
    <id>cloudbees-snapshot-repository</id>
    <url>https://repository-hazelcast-l337.forge.cloudbees.com/snapshot/</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

### Building From Source

Pull latest from repo `git pull origin master` and use Maven install (or package) to build `mvn clean install`.

### Testing

Type `mvn test` to run unit and integration tests.

### Checkstyle and Findbugs

In each Pull Request, we do static analyzing on the changes.
Run the following commands locally to check if your contribution is checkstyle and findbugs compatible.

```
mvn clean compile -P findbugs
```

```
mvn clean validate -P checkstyle
```

### Contributing to Hazelcast Jet

We encourage pull requests and process them promptly.

To contribute:

* see [Developing with Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process
* complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)

For an enhancement or larger feature, create a GitHub issue first to discuss.

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast is available under the Apache 2 License. Please see the [Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) for more information.

#### Copyright

Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.