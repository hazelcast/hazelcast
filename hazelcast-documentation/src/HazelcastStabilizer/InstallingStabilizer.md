

## Installing Stabilizer

*Current Edit:*You can download the compressed file containing the stabilizer artifacts [here](http://search.maven.org/remotecontent?filepath=com/hazelcast/stabilizer/hazelcast-stabilizer-dist/0.3/hazelcast-stabilizer-dist-0.3-dist.zip).

*Should be:* Hazelcast Stabilizer is provided as a separate downloadable package, in `zip` or `tar.gz` format. You can download the either one [here](???).

After the download is completed, follow the below steps (*note by Serdar: the step `update_stabilizer` is skipped here since I assume the Stabilizer ZIP will handle that*):

- Unpack the `tar.gz` or `zip` file to a directory which you prefer to be the home directory of Hazelcast Stabilizer. It extracts with the name `hazelcast-stabilizer-<`*version*`>` into this directory.

- Add the following lines to the file `~/.bashrc` (for Linux) or to the file `~/.profile` (for Mac OSX).

```
export STABILIZER_HOME=<extracted directory path>/hazelcast-stabilizer-<version>
PATH=$STABILIZER_HOME/bin:$PATH
```

- Create a working directory for your Stabilizer tests (`tests` is an example name in the following command).

```
mkdir ~/tests
```

### Setting Public/Private Key Pair

Having set up Hazelcast Stabilizer as described in the previous section, make sure you have the file `id_rsa.pub` in your `~/.ssh` directory. The [Provisioner](#provisioner) terminates when it needs to access the cloud and realizes that the public/private key pair is missing. If you do not have any, you can generate a public/private key pair using the following command.

```
ssh-keygen -t rsa -C "your_email@example.com"
```

Press `Enter` for all questions. The value for the e-mail address is not relevant in this case. After you execute this command, you should have the files `id_rsa.pub` and `id_rsa` in your `~/.ssh` directory. The key `id_rsa.pub` is copied to the remote agent machines automatically and added to the file `~/.ssh/known_hosts`. By this way, you can log into a machine without a password or explicitly provided credentials.
