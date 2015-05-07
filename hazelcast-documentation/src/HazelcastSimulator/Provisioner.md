

## Provisioner

The provisioner is responsible for provisioning (starting/stopping) instances in a cloud. It will start an Operating
System instance, install Java, open firewall ports and install Simulator Agents.

You can configure the behavior of the cluster—such as cloud, operating system, hardware, JVM version, Hazelcast version or region—through
the file `simulator.properties`. Please see the [Simulator.Properties File Description section](#simulator-properties-file-description) for more information. 

You can use the following arguments with the `provisioner`.

To start a cluster:

```
provisioner --scale 1
```

To scale to a 2 member cluster:

```
provisioner --scale 2
```

To scale back to a 1 member cluster:

```
provisioner --scale 1
```

To terminate all members in the cluster:

```
provisioner --terminate
```

or

```
provisioner --scale 0
```

If you want to restart all agents and also upload the newest JARs to the machines:

```
provisioner --restart
```

To download all the worker home folders (containing logs and whatever has been put inside):

```
provisioner --download
```
This command is also useful if you added a profiling because the profiling information will also be downloaded. The command is also useful when an out of memory exception is thrown because you can download the heap dump.


To remove all the worker home directories:

```
provisioner --clean
```

### Accessing the Provisioned Machine

When a machine is provisioned, a user with the name `simulator` is created on the remote machine by default, and that user is added
to the sudousers list. Also, the public key of your local user is copied to the remote machine and added to the file 
`~/.ssh/authorized_keys`. You can login to that machine using the following command.

```
ssh simulator@ip
```

You can change the name of the created user to something else by setting the `USER=<somename>` property in the file `simulator.properties`. Be careful not to pick a name that is used on the target image: for example, if you use `ec2-user/ubuntu`, and the
default user of that image is `ec2-user/ubuntu`, then you can run into authentication problems.