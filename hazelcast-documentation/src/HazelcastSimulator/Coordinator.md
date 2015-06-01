

## Coordinator

The Coordinator is responsible for actually running the test using the agents.

You can deploy your test on the workers using the following command.

```
coordinator yourtest.properties.
```

This command creates a single worker per agent and runs the test for 60 seconds (the default duration for a Hazelcast Simulator test).

If your test properties file is called `test.properties`, then you can use the following command to have the coordinator pick up your `test.properties` file automatically.

```
coordinator
```

### Controlling Hazelcast Declarative Configuration

By default, the coordinator uses the files `SIMULATOR_HOME/conf/hazelcast.xml` and `SIMULATOR_HOME/conf/
client-hazelcast.xml`
to generate the correct Hazelcast configuration. To use your own configuration files instead, use the following arguments:

```
coordinator --clientHzFile=your-client-hazelcast.xml --hzFile your-hazelcast.xml ....
```



### Controlling Test Duration

You can control the duration of a single test using the `--duration` argument. The default duration is 60 seconds. You can specify your own durations using *m* for minutes, *d* for days or *s* for seconds with this argument.

You can see the usage of the `--duration` argument in the following example commands.

```
coordinator --duration 90s  map.properties
```

```
coordinator --duration 3m  map.properties
```

```
coordinator --duration 12h  map.properties
```

```
coordinator --duration 2d  map.properties
```

### Controlling Client And Workers

By default, the provisioner starts the cluster members. You can also use the `--memberWorkerCount` and `--clientWorkerCount` arguments to control how many members and clients you want to have.

The following command creates a 4 node Hazelcast cluster and 8 clients, and all load will be generated through the clients. It also runs the `map.properties` test for a duration of 12 hours. 

```
coordinator --memberWorkerCount 4 --clientWorkerCount 8 --duration 12h  map.properties
```

Profiles are usually configured with some clients and some members. If you want to have members and no clients:

```
coordinator --memberWorkerCount 12  --duration 12h  map.properties
```

If you want to have a JVM with embedded client plus member and all communication goes through the client:

```
coordinator --mixedWorkerCount 12  --duration 12h  map.properties
```

If you want to run 2 member JVMs per machine:

```
coordinator --memberWorkerCount 24  --duration 12h  map.properties
```

As you notice, you can play with the actual deployment.