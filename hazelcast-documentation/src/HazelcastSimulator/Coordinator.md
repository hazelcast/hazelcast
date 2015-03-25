

## Coordinator

The Coordinator is responsible for actually running the test using the agents.

You can deploy your test on the workers using the following command.

```
coordinator yourtest.properties.
```

This creates a single worker per agent and runs the test for 60 seconds. This is the default duration for a Hazelcast Simulator test.

If your test properties file is called `test.properties`, then the coordinator picks it up automatically, using just the command `coordinator`.


### Controlling Hazelcast Declarative Configuration

By default, the coordinator makes use of the files `SIMULATOR_HOME/conf/hazelcast.xml` and `SIMULATOR_HOME/conf/client-hazelcast.xml`
to generate the correct Hazelcast configuration. You can use your own configuration by overriding these files using the following arguments:

```
coordinator --clientHzFile=your-client-hazelcast.xml --hzFile your-hazelcast.xml ....
```



### Controlling Test Duration

The duration of a single test can be controlled using the `--duration` argument. The default duration is 60 seconds. However, you can specify your own durations using *m* for minutes, *d* for days or *s* for seconds with this argument.

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

By default, the provisioner starts the cluster members. You can also use `--memberWorkerCount` and `--clientWorkerCount` arguments to control how many members and clients you want to have. Please see the following example command.  

```
coordinator --memberWorkerCount 4 --clientWorkerCount 8 --duration 12h  map.properties
```

The above command creates a 4 node Hazelcast cluster and 8 clients, and all load will be generated through the clients. It also runs the `map.properties` test for a duration of 12 hours. 

One of the suggestions is that currently the profiles are configured with X clients and Y servers.

If you want to have members and no clients:

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