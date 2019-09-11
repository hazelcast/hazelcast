# Configure Logging

The samples in this module demonstrate how to configure logging for Hazelcast Jet.

## Building the application

```
mvn clean package
```

## Running the application

To start the application with each logging configuration option, execute 
the following commands:

### JDK Logging

```
./start-jdk.sh
```

### Log4j Logging

```
./start-log4j.sh
```

### Slf4j Logging

```
./start-slf4j.sh
```

### No Logging

```
./start-none.sh
```

For more information about logging, please refer to the [Logging Configuration](https://docs.hazelcast.org/docs/jet/latest/manual/#configure-logging)
section on reference manual.