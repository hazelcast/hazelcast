

## Provisioner

The provisioner is responsible for provisioning (starting/stopping) instances in a cloud. It will start an Operating
System instance, install Java, open firewall ports and install Stabilizer Agent.

The behavior of the cluster like cloud, os, hardware, jvm version, Hazelcast version or region can be configured through
the stabilizer.properties. 

To start a cluster:

```
provisioner --scale 1
```

To scale to 2 member cluster:

```
provisioner --scale 2
```

To scale back 1 member:

```
provisioner --scale 1
```

To terminate all members in the cluster

```
provisioner --terminate
```

or

```
provisioner --scale 0
```

If you want to restart all agents and also upload the newest jars to the machines:

```
provisioner --restart
```

To download all worker home directories (containing logs and whatever has been put inside). This command also is useful
if you added profiling so that the profiling information is downloaded. And when a OOME has happened so you can download
the heap dump.

```
provisioner --download
```

To remove all the worker home directories

```
provisioner --clean
```

### Accessing the provisioned machine

When a machine is provisioned, by default a user with the name 'stabilizer' is create on the remote machine and added
to the sudoers list. Also the public key of your local user is copied to the remote machine and added to
~/.ssh/authorized_keys. So you can login to that machine using:

```
ssh stabilizer@ip
```

You can change name of the created user to something else in by setting the "USER=somename" property in the stabilizer
properties. Be careful not to pick a name that is used on the target image. E.g. if you use ec2-user/ubuntu, and the
default user of that image is ec2-user/ubuntu, then you can run into authentication problems. So probably it is best
not to change this value, unless you know what your are doing.

