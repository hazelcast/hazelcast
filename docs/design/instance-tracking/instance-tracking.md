# Instance tracking

## Background
### Description
This document outlines the solution for adding instance tracking to Hazelcast IMDG. Instance tracking is a feature which, when enabled, writes a file on instance startup at the configured location. The file will contain metadata about the instance, such as version, product name, process ID and is not deleted on node shutdown. This file can then later be used by other programs to detect what kind of Hazelcast instances were running on a particular machine.

## Functional Design
### Summary of Functionality

The user will be able to track what kind of instances were started on a particular machine by inspecting the contents of a file, or a set of files, written during instance startup. Each instance startup writes a new file, or overwrites an existing file, and contains metadata about the instance being started. 

The file name and the file contents are configurable and may contain placeholders. The placeholders have a prefix to be able to distinguish between a placeholder for the instance tracking feature as opposed to some other placeholder like XML placeholders. We use the same style as the `EncryptionReplacer` by adding a "namespace" to the placeholder prefix. For example, `$HZ_INSTANCE_TRACKING{start_timestamp}` (the namespace here being `HZ_INSTANCE_TRACKING`).

Here is an example of programmatic Java configuration:
```java
Config config = new Config();
config.getInstanceTrackingConfig()
      .setEnabled(true)
      .setFileName("/tmp/hz-tracking.txt")
      .setFormatPattern("$HZ_INSTANCE_TRACKING{product}:$HZ_INSTANCE_TRACKING{version}");
```

The equivalent XML configuration:
```xml
<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-5.2.xsd">
    
    <instance-tracking enabled="true">
        <file-name>/tmp/hz-tracking.txt</file-name>
        <format-pattern>$HZ_INSTANCE_TRACKING{product}:$HZ_INSTANCE_TRACKING{version}</format-pattern>
    </instance-tracking>

</hazelcast>
```

The equivalent YAML configuration:
```yaml
hazelcast:
  instance-tracking:
    enabled: true
    file-name: /tmp/hz-tracking.txt
    format-pattern: $HZ_INSTANCE_TRACKING{product}:$HZ_INSTANCE_TRACKING{version}
```

The feature supports both OS and EE members and clients, is disabled by default and uses a JVM argument to distinguish if the instance was started using `start.sh/bat` or inside another application by invoking `Hazelcast.newHazelcastInstance()`.

The file overwrites any existing file with the same name. Placeholders in the file name can be used to create a new file each time. Failing to write the file will only generate a warning and the instance is allowed to start.

## Technical Design

We introduce new configuration both on the member and the client side:
```java
/**
 * Configures tracking of a running Hazelcast instance. For now, this is
 * limited to writing information about the Hazelcast instance to a file
 * while the instance is starting.
 * <p>
 * The file is overwritten on every start of the Hazelcast instance and if
 * multiple instance share the same file system, every instance will
 * overwrite the tracking file of a previously started instance.
 * <p>
 * If this instance is unable to write the file, the exception is logged and
 * the instance is allowed to start.
 *
 * @since 4.1
 */
public class InstanceTrackingConfig {
    private boolean enabled;
    private String fileName;
    private String formatPattern;
}
```

The user can use this configuration to enable the instance tracking feature, specify the file name and the pattern for the file contents. By default, the feature is disabled, the file name is `Hazelcast.process` in the OS temporary directory as returned by `System.getProperty("java.io.tmpdir")` and the file contents are JSON-formatted key-value pairs of all available metadata.

Here is an example when running a client instance:
```json
{"product":"Hazelcast", "version":"4.1.7", "pid":27746, "mode":"client", "start_timestamp":1595851430741, "licensed":0}
```

Here is an example when running a member instance in the "server" mode:
```json
{"product":"Hazelcast", "version":"4.1.7", "pid":27746, "mode":"server", "start_timestamp":1595851430741, "licensed":1}
```

And here is an example when running a member instance in the "embedded" mode:
```json
{"product":"Hazelcast", "version":"4.1.7", "pid":27746, "mode":"embedded", "start_timestamp":1595851430741, "licensed":1}
```

The user can specify a custom format by using a predefined set of available metadata keys. For example:
```java
String format = "mode: $HZ_INSTANCE_TRACKING{mode}\n"
        + "product: $HZ_INSTANCE_TRACKING{product}\n"
        + "licensed: $HZ_INSTANCE_TRACKING{licensed}\n"
        + "missing: $HZ_INSTANCE_TRACKING{missing}\n"
        + "broken: $HZ_INSTANCE_TRACKING{broken ";
```

This should produce a file with this content:
```
mode: embedded
product: Hazelcast
licensed: 0
missing: $HZ_INSTANCE_TRACKING{missing}
broken: $HZ_INSTANCE_TRACKING{broken
```

As can be seen, once we encounter a broken placeholder, all subsequent placeholders are ignored. On the other hand, missing placeholders are skipped and subsequent placeholders are resolved. Here is a list of the currently valid metadata placeholders and their possible values:

```java
/**
 * Enumeration of instance properties provided to the format pattern for
 * output.
 */
public enum InstanceTrackingProperties {
    /**
     * The instance product name, e.g. "Hazelcast" or "Hazelcast Enterprise".
     */
    PRODUCT("product"),
    /**
     * The instance version.
     */
    VERSION("version"),
    /**
     * The instance mode, e.g. "server", "embedded" or "client".
     */
    MODE("mode"),
    /**
     * The timestamp of when the instance was started as returned by
     * {@link System#currentTimeMillis()}.
     */
    START_TIMESTAMP("start_timestamp"),
    /**
     * If this instance is using a license or not. The value {@code 0} signifies
     * that there is no license set and the value {@code 1} signifies that a
     * license is in use.
     */
    LICENSED("licensed"),
    /**
     * Attempts to get the process ID value. The algorithm does not guarantee to
     * get the process ID on all JVMs and operating systems so please test before
     * use.
     * In case we are unable to get the PID, the value will be {@code -1}.
     */
    PID("pid");
}
```

The possible values for the `product` placeholder are: `Hazelcast`, `Hazelcast Enterprise`, `Hazelcast Client`, `Hazelcast Client Enterprise`, `Hazelcast Jet`, `Hazelcast Jet Enterprise`. 
The possible values for the `mode` placeholder are:
 - `server` - this value is used when the instance was started using the `start.sh` or `start.bat` scripts. If the JVM argument `hazelcast.tracking.server` is set to `true`, we conclude that the instance was started in server mode.
 - `client` - this instance is a Hazelcast client instance
 - `embedded` - this instance is embedded in another Java program. This will be the case when the JVM argument `hazelcast.tracking.server` is not set to `true`.
 
In addition to the above, the hazelcast instance will overwrite any existing file in the configured location. To prevent this, the user can configure the file location using the placeholders in the same way they can be used when defining the file contents. For example, if the file name is configured as `Hazelcast-$HZ_INSTANCE_TRACKING{pid}-$HZ_INSTANCE_TRACKING{start_timestamp}.process`, the file name will contain the process ID and the creation time, making it unique every time the instance is started. The created file is not deleted on node shutdown. As such, it leaves a trace of instances started on a particular machine. The file creation process also is fail-safe meaning that the instance will proceed with starting even though it is unable to write the tracking file and the instance will only log a warning.

### Testing criteria

We use regular unit tests to test the changes introduced by this functionality.