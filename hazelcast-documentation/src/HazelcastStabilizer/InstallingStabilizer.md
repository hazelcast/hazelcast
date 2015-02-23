

## Installing Stabilizer

*Current Edit:*You can download the compressed file containing the stabilizer artifacts [here](http://search.maven.org/remotecontent?filepath=com/hazelcast/stabilizer/hazelcast-stabilizer-dist/0.3/hazelcast-stabilizer-dist-0.3-dist.zip).

*Should be:* Hazelcast Stabilizer is provided as a separate downloadable package, in `zip` or `tar.gz` format. You can download the either one [here](???).

After the download is completed, follow the below steps (*note by Serdar: the step `update_stabilizer` is skipped here since I assume the Stabilizer ZIP will handle that*):

- Unpack the `tar.gz` or `zip` file to your preferred directory. It extracts with the name `hazelcast-stabilizer-<`*version*`>` into that directory.

- Add the following lines to the file `~/.bashrc` (for Linux) or to the file `~/.profile` (for Mac OSX).

```
export STABILIZER_HOME=<extracted directory path>/hazelcast-stabilizer-<version>
PATH=$STABILIZER_HOME/bin:$PATH
```

- Create a working directory for your Stabilizer tests (`tests` is an example name in the following command).

```
mkdir ~/tests
```

- Copy the file `STABILIZER_HOME/conf/stabilizer.properties` to your working directory and edit this file. For example, if your Hazelcast cluster is on Amazon EC2, you only need to set your EC2 identity and credential, using the parameters shown below and included in the `stabilizer.properties` file:

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

You should create the files `~/ec2.identity` and `~/ec2.credential` that contain your EC2 access key and secret key, respectively. 

![image](images/NoteSmall.jpg) ***NOTE***: *Creating these files instead of just setting the access and secret keys in the `stabilizer.properties` file is for security reasons. It is too easy to share your credentials with the outside world and now you can safely add the `stabilizer.properties` file in your source repository or share it with other people.*


### Setting Public/Private Key Pair

Having set up Hazelcast Stabilizer as described in the previous section, make sure you have the file `id_rsa.pub` in your `~/.ssh` directory. The [Provisioner](#provisioner) terminates when it needs to access the cloud and realizes that the public/private key pair is missing. If you do not have any, you can generate a public/private key pair using the following command.

```
ssh-keygen -t rsa -C "your_email@example.com"
```

Press `Enter` for all questions. The value for the e-mail address is not relevant in this case. After you execute this command, you should have the files `id_rsa.pub` and `id_rsa` in your `~/.ssh` directory. The key `id_rsa.pub` is copied to the remote agent machines automatically and added to the file `~/.ssh/known_hosts`. By this way, you can log into a machine without a password or explicitly provided credentials.

### Install wget

Make sure wget is installed. For Linux machines in most cases it is installed, but on OSX it isn't out of the box.

### Set up on Google Compute Engine

For setup on GCE a developer email can be obtained from the GCE admin GUI console. Its usually something in the form: @developer.gserviceaccount.com go to API & Auth > Credentials, click Create New Client ID, select Service Account. save your p12 keystore file that is obtained when creating a "Service Account"

run ./setupGce.sh script provide you @developer.gserviceaccount.com as 1st argument provide and the path to your p12 file as 2nd argument the setupGce.sh script will create your stabilizer.properties, in the conf directory