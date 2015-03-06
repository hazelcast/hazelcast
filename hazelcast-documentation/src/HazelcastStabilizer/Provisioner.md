

## Provisioner

The provisioner is responsible for provisioning (starting/stopping) instances in a cloud. It will start an Operating
System instance, install Java, open firewall ports and install Simulator Agents.

The behavior of the cluster like cloud, operating system, hardware, JVM version, Hazelcast version or region can be configured through
the file `simulator.properties`. Please see the [Simulator.Properties File Description section](#simulator-properties-file-description) for more information. 

The following are the arguments you can use with the `provisioner`.

To start a cluster:

```
provisioner --scale 1
```

To scale to 2 member cluster:

```
provisioner --scale 2
```

To scale back to 1 member cluster:

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

To download all the worker home directories (containing logs and whatever has been put inside):

```
provisioner --download
```
This command is also useful
if you added a profiling so that the profiling information is downloaded and when an out of memory exception is thrown so you can download
the heap dump.


To remove all the worker home directories

```
provisioner --clean
```

### Accessing the Provisioned Machine

When a machine is provisioned, by default a user with the name `simulator` is created on the remote machine and added
to the sudousers list. Also, the public key of your local user is copied to the remote machine and added to the file 
`~/.ssh/authorized_keys`. You can login to that machine using the following command.

```
ssh stabilizer@ip
```

You can change the name of the created user to something else by setting the `USER=<somename>` property in the file `simulator.properties`. Be careful not to pick a name that is used on the target image, e.g. if you use `ec2-user/ubuntu`, and the
default user of that image is `ec2-user/ubuntu`, then you can run into authentication problems.