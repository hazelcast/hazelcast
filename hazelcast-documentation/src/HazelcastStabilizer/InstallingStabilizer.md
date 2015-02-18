

## Installing Stabilizer

The zip/tar.gz file containing the stabilizer artifacts can be downloaded here:

http://search.maven.org/remotecontent?filepath=com/hazelcast/stabilizer/hazelcast-stabilizer-dist/0.3/hazelcast-stabilizer-dist-0.3-dist.zip

Download and unpack the tar.gz or zip file to e.g. the home directory.

add to ~/.bashrc when using Linux or ~/.profile when using OSX:

export STABILIZER_HOME=~/hazelcast-stabilizer-0.3
PATH=$STABILIZER_HOME/bin:$PATH

Create your tests working directory, e.g.

mkdir ~/tests

Copy the STABILIZER_HOME/conf/stabilizer.properties to the tests directory. And make the changes required. In case of EC2, you only need to set your ec2 identity/credential:

CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential

The simplest thing you can do is to make a file '~/ec2.identity' containing your access key. And another file '~/ec2.credential' containing your secret key. The reason why you need to create files instead of just setting the values in this the stabilizer.properties, is security: it is too easy to share your credentials with the outside world and now you can safely add the stabilizer.properties file in your source repo or share it with other people.

### Setup public/private-key

After you have set up stabilizer, make sure you have a id_rsa.pub in your ~/.ssh directory. The Provisioner will terminate when it needs to access the cloud and sees that the public/private key are missing. If you don't have a public/private key, you can generate one like this:

ssh-keygen -t rsa -C "your_email@example.com"
You can press enter on all questions. The value for the email address is not relevant. After this command has completed, you should have a ida_rsa.pub and id_rsa file in your ~/.ssh directory. Your id_rsa.pub key will automatically be copied to the remote agent machines and added to the ~/.ssh/known_hosts file, so that you can log into that machine without a password or explicit provided credentials.

### Install wget

Make sure wget is installed. For Linux machines in most cases it is installed, but on OSX it isn't out of the box.

### Set up on Google Compute Engine

For setup on GCE a developer email can be obtained from the GCE admin GUI console. Its usually something in the form: @developer.gserviceaccount.com go to API & Auth > Credentials, click Create New Client ID, select Service Account. save your p12 keystore file that is obtained when creating a "Service Account"

run ./setupGce.sh script provide you @developer.gserviceaccount.com as 1st argument provide and the path to your p12 file as 2nd argument the setupGce.sh script will create your stabilizer.properties, in the conf directory