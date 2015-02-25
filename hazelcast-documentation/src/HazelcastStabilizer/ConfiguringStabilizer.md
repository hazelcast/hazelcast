

## Configuring Stabilizer

After you installed the Stabilizer, you may want to configure it to prepare the Stabilizer tests for a proper execution in your environment.

You can use the following files to configure the Stabilizer tests:

- stabilizer.properties
- test.properties


### stabilizer.properties

???


Example configuration. The only thing you need to do to run in EC2 is to set the CLOUD_IDENTITY and CLOUD_CREDENTIAL.

For a full listing of options, check out the STABILIZER_HOME/conf/stabilizer.properties.

CLOUD_PROVIDER=aws-ec2
The file containing your aws access key

CLOUD_IDENTITY=~/ec2.identity
The file containing your aws identity

CLOUD_CREDENTIAL=~/ec2.credential
MACHINE_SPEC=hardwareId=m3.medium,locationId=us-east-1,imageId=us-east-1/ami-fb8e9292
JDK_FLAVOR=oracle
JDK_VERSION=7
PROFILER=none
HAZELCAST_VERSION_SPEC=outofthebox



### test.properties


????


class=${package}.ExampleTest
threadCount=10
logFrequency=10000
performanceUpdateFrequency=10000



