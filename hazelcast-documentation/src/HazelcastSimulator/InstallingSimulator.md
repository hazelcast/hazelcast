
## Installing Simulator

Hazelcast Simulator needs a UNIX shell to run. So ensure your local and remote machines are running under Unix, Linux or Mac OS. Windows may work with additions like Cygwin, but is not officially supported at the moment.

### Setup of local machine (Coordinator)

*Current Edit:* You can download the compressed file containing the simulator artifacts [here](http://search.maven.org/remotecontent?filepath=com/hazelcast/simulator/hazelcast-simulator-dist/0.3/hazelcast-simulator-dist-0.3-dist.zip).

*Should be:* Hazelcast Simulator is provided as a separate downloadable package, in `zip` or `tar.gz` format. You can download the either one [here](http://www.hazelcast.org/download).

After the download is completed, follow the below steps.

- Unpack the `tar.gz` or `zip` file to a directory which you prefer to be the home directory of Hazelcast Simulator. It extracts with the name `hazelcast-simulator-<`*version*`>` into this directory. Do this also to update Hazelcast Simulator (skip the following steps).

- Add the following lines to the file `~/.bashrc` (for Unix/Linux) or to the file `~/.profile` (for Mac OS).

```
export SIMULATOR_HOME=<extracted directory path>/hazelcast-simulator-<version>
PATH=$SIMULATOR_HOME/bin:$PATH
```

- Create a working directory for your Simulator `TestSuite` (`tests` is an example name in the following commands).

```
mkdir ~/tests
```

- Copy the `simulator.properties` to your working directory.

```
cp $SIMULATOR_HOME/conf/simulator.properties ~/tests
```

### Setup of remote machines (Agents, Workers)

Having installed Hazelcast Simulator as described in the previous section, make sure you create a user on the remote machines you want to run `Agents` and `Workers` on. The default username used by Hazelcast Simulator is `simulator`. You can change this in the `simulator.properties` file in your working directory.

Please ensure that you can connect to the remote machines with the configured username and without password authentication (see next section). The [Provisioner](#provisioner) terminates when it needs to access the remote machines and cannot connect automatically.

### Setup of public/private key pair

The preferred way for password free authentication is the usage of a RSA public/private key pair. The usage of the RSA key should not require to enter the pass-phrase manually. A key with pass-phrase and ssh-agent-forwarding is strongly recommended, but a pass-phrase-less key will also work.

#### Local machine (Coordinator)

Make sure you have the files `id_rsa.pub` and `id_rsa` in your local `~/.ssh` directory.

If you do not have RSA keys, you can generate a public/private key pair using the following command.

```
ssh-keygen -t rsa -C "your_email@example.com"
```

Press `[Enter]` for all questions. The value for the e-mail address is not relevant in this case. After you execute this command, you should have the files `id_rsa.pub` and `id_rsa` in your `~/.ssh` directory.

#### Remote machines (Agents, Workers)

Please ensure you have appended the public key (`id_rsa.pub`) to the `~/.ssh/authorized_keys` file on all remote machines (`Agents` and `Workers`). You can 
copy the public key to all your remote machines using the following command.

```
ssh-copy-id -i ~/.ssh/id_rsa.pub simulator@remote-ip-address
```

#### Connection test

You can check if the connection works as expected using the following command from the `Coordinator` machine (will print "ok" if everything is fine).

```
ssh -o BatchMode=yes simulator@remote-ip-address "echo ok" 2>&1
```
