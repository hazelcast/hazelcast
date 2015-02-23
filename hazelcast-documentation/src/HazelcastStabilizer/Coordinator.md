

## Coordinator

The Coordinator is responsible for actually running the test using the agents.

Deploying a test on workers is as simple as:

```
coordinator yourtest.properties.
```

This will create a single worker per agent and run the test for 60 seconds.

If your test properties file is called 'test.properties', then the coordinator will pick it up automatically:

```
coordinator
```

### Controlling the Hazelcast xml configuration

By default the coordinator makes use of STABILIZER_HOME/conf/hazelcast.xml and STABILIZER_HOME/conf/client-hazelcast.xml
to generate the correct Hazelcast configuration. But you can override this, so you can use your own configuration:

coordinator --clientHzFile=your-client-hazelcast.xml --hzFile your-hazelcast.xml ....

### Controlling duration:
The duration of a single test can be controlled using the --duration setting, which defaults to 60 seconds.

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

### Controlling client/workers:

By default the provisioner will only start members, but you can control how many clients you want to have.

```
coordinator --memberWorkerCount 4 --clientWorkerCount 8 --duration 12h  map.properties
```

In this case we create a 4 node Hazelcast cluster and 8 clients and all load will be generated through the clients. We
run the map.properties test for a duration of 12 hours. Also m for minutes, d for days or s for seconds can be used.

One of the suggestions is that currently the profiles are configured with X clients and Y servers.

But it could be that you only want to have servers and no clients:

```
coordinator --memberWorkerCount 12  --duration 12h  map.properties
```

Or maybe you want to have a JVM with embedded client + server but all communication goes through the client:

```
coordinator --mixedWorkerCount 12  --duration 12h  map.properties
```

Or maybe you want to run 2 member JVMs per machine:

```
coordinator --memberWorkerCount 24  --duration 12h  map.properties
```

You can very easily play with the actual deployment.