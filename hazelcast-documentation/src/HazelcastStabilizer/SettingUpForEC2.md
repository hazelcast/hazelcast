

## Setting Up For Amazon EC2

Having installed it, this section describes how to prepare the Stabilizer for testing a Hazelcast cluster deployed at Amazon EC2.

- Copy the file `STABILIZER_HOME/conf/stabilizer.properties` to your working directory and edit this file. For example, if your Hazelcast cluster is on Amazon EC2, you only need to set your EC2 identity and credential, using the parameters shown below and included in the `stabilizer.properties` file:

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

You should create the files `~/ec2.identity` and `~/ec2.credential` that contain your EC2 access key and secret key, respectively. 

![image](images/NoteSmall.jpg) ***NOTE***: *Creating these files instead of just setting the access and secret keys in the `stabilizer.properties` file is for security reasons. It is too easy to share your credentials with the outside world and now you can safely add the `stabilizer.properties` file in your source repository or share it with other people.*

