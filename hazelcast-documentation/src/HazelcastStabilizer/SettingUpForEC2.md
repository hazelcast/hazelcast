

## Setting Up For Amazon EC2

Having installed it, this section describes how to prepare the Simulator for testing a Hazelcast cluster deployed at Amazon EC2. 

To do this, you need to copy the file `SIMULATOR_HOME/conf/simulator.properties` to your working directory and edit this file. You should set the values for the following parameters included in this file.

- CLOUD_PROVIDER: Maven artifact ID of the cloud provider. In this case it is `aws-ec2` for Amazon EC2. Please refer to the [Simulator.Properties File Description section](#simulator-properties-file-description) for a full list of cloud providers.
- CLOUD_IDENTITY: The path to the file that contains your EC2 access key. 
- CLOUD_CREDENTIAL: The path to the file that contains your EC2 secret key. 
- MACHINE_SPEC: The parameter by which you can specify the EC2 instance type, operating system of the instance, EC2 region, etc. 

The following is an example of a `simulator.properties` file with the parameters explained above.


```
CLOUD_PROVIDER=aws-ec2
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
MACHINE_SPEC=hardwareId=c3.xlarge,imageId=us-east-1/ami-1b3b2472
```

For the above example, you should have created the files `~/ec2.identity` and `~/ec2.credential` that contain your EC2 access key and secret key, respectively. 

![image](images/NoteSmall.jpg) ***NOTE***: *Creating these files instead of just setting the access and secret keys in the `simulator.properties` file is for security reasons. It is too easy to share your credentials with the outside world and now you can safely add the `simulator.properties` file in your source repository or share it with other people.*

![image](images/NoteSmall.jpg) ***NOTE***: *For the full description of `simulator.properties` file, please refer to the [Simulator.Properties File Description section](#simulator-properties-file-description).*


