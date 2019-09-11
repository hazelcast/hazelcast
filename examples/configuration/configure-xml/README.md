# XML Configuration

The samples in this module demonstrate how to configure a Hazelcast Jet cluster with XML configuration.

### Configuration Lookup

Creating member instance with `Jet.newJetInstance()` looks for configuration files
in multiple locations both in XML and YAML format. XML configuration takes precedence
over YAML configuration in all locations resulting the following lookup order:
1. The configuration passed in `hazelcast.jet.config` JVM argument, either XML or YAML
2. `hazelcast-jet.xml` on the classpath
3. `hazelcast-jet.xml` in the working directory
4. `hazelcast-jet.yaml` on the classpath
5. `hazelcast-jet.yaml` in the working directory
6. default configuration `hazelcast-jet-default.xml`

Similarly, the (Java) client instances created with `Jet.newJetClient()` follows
the same lookup logic and order:
1. The configuration passed in `hazelcast.client.config` JVM argument, either XML or YAML
2. `hazelcast-client.xml` on the classpath
3. `hazelcast-client.xml` in the working directory
4. `hazelcast-client.yaml` on the classpath
5. `hazelcast-client.yaml` in the working directory
6. default configuration `hazelcast-jet-client-default.xml`

The sample `ConfigLookup` demonstrates how the configuration files are located on the classpath and 
how to use the locator logic to take only YAML configuration files into account. 

### Loading XML Configuration Without Lookup

If it is known that the cluster will be configured with XML configuration and no need for using the location 
logic shown in the first sample, the following XML-specific config loader utility methods on the `JetConfig` 
class can be used:
- `JetConfig.loadFromClasspath()`: loads the configuration from the classpath based on the provided filename
- `JetConfig.loadFromFile()`     : loads the configuration from the file system based on the provided filename
- `JetConfig.loadXmlFromStream()`: loads the configuration from the provided input stream
- `JetConfig.loadXmlFromString()`: loads the configuration from the provided `String` containing XML configuration 

The sample `XmlJetConfigClasspath` loads the jet configuration `hazelcast-jet-sample.xml` from the classpath and 
demonstrates variable replacement in XML configuration by defining the backup count and metrics configuration
in variables taken from the system properties and then from a provided `Properties` instance.   

#### Full XML Configuration Example

There is a full example XML configuration in the distributed Hazelcast Jet jars, named `hazelcast-jet-full-example.xml`.
This example file can be used for reference when writing XML configuration.
