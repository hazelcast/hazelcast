

## Writing a Stabilizer Test


Probably you want to write your own test. The easiest way to do that is to make use of the Stabilizer archetype which
will generate a project for you.

```
mvn archetype:generate  \
    -DarchetypeGroupId=com.hazelcast.stabilizer \
    -DarchetypeArtifactId=archetype \
    -DarchetypeVersion=0.3 \
    -DgroupId=yourgroupid  \
    -DartifactId=yourproject
```

This will create fully working stabilizer project including a very basic test: see ExampleTest that Increments an
IAtomicLong. When the test has completed, a verification is done if the actual number of increments is equal to the
expected number of increments. In your case you probably want to do something more interesting.

After this project is generated, go to the created directory and run:

```
mvn clean install
```

And then go to workdir:
 
```
cd workdir
```
 
Edit the stabilizer.properties file. In case of EC2, you only need to have a look at the following 2 properties:

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

So create these two text files in your home directory. The ec2.identity text-file should contain your access key and the 
ec2.credential text-file your secret key. For a full listing of options and explanation of the options, read the 
STABILIZER_HOME/conf/stabilizer.properties. This file contains all the default values that are used as soon as 
a specific stabilizer.properties doesn't contain a specific property.

And finally you can run the test from the workdir directory:

```
./run.sh
```

This script will:
 * start 4 EC2 instances, install Java, install the agents.
 * upload your jars, run the test using a 2 node test cluster and 2 client machines (the clients generate the load).
   This test will run for 2 minutes.
 * After the test completes the the artifacts (log files) are downloaded in the 'workers' directory
 * terminate the 4 created instances. If you don't want to start/terminate the instances for every run, just comment out
   'provisioner --terminate' line.  This prevents the machines from being terminated.

The output will look something like this:

```
INFO  08:40:10 Hazelcast Stabilizer Provisioner
INFO  08:40:10 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:40:10 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3
INFO  08:40:10 Loading stabilizer.properties: /tmp/yourproject/workdir/stabilizer.properties
INFO  08:40:10 ==============================================================
INFO  08:40:10 Provisioning 4 aws-ec2 machines
INFO  08:40:10 ==============================================================
INFO  08:40:10 Current number of machines: 0
INFO  08:40:10 Desired number of machines: 4
INFO  08:40:10 GroupName: stabilizer-agent
INFO  08:40:10 JDK spec: oracle 7
INFO  08:40:10 Hazelcast version-spec: outofthebox
INFO  08:40:12 Created compute
INFO  08:40:12 Machine spec: hardwareId=m3.medium,locationId=us-east-1,imageId=us-east-1/ami-fb8e9292
INFO  08:40:19 Security group: 'stabilizer' is found in region 'us-east-1'
INFO  08:40:27 Created template
INFO  08:40:27 Loginname to the remote machines: stabilizer
INFO  08:40:27 Creating machines (can take a few minutes)
INFO  08:42:10 	54.91.98.103 LAUNCHED
INFO  08:42:10 	54.237.144.164 LAUNCHED
INFO  08:42:10 	54.196.60.36 LAUNCHED
INFO  08:42:10 	54.226.58.200 LAUNCHED
INFO  08:42:24 	54.196.60.36 JAVA INSTALLED
INFO  08:42:25 	54.237.144.164 JAVA INSTALLED
INFO  08:42:27 	54.91.98.103 JAVA INSTALLED
INFO  08:42:30 	54.226.58.200 JAVA INSTALLED
INFO  08:42:57 	54.196.60.36 STABILIZER AGENT INSTALLED
INFO  08:42:59 	54.237.144.164 STABILIZER AGENT INSTALLED
INFO  08:43:01 	54.196.60.36 STABILIZER AGENT STARTED
INFO  08:43:02 	54.237.144.164 STABILIZER AGENT STARTED
INFO  08:43:06 	54.91.98.103 STABILIZER AGENT INSTALLED
INFO  08:43:09 	54.91.98.103 STABILIZER AGENT STARTED
INFO  08:43:21 	54.226.58.200 STABILIZER AGENT INSTALLED
INFO  08:43:25 	54.226.58.200 STABILIZER AGENT STARTED
INFO  08:43:25 Duration: 00d 00h 03m 15s
INFO  08:43:25 ==============================================================
INFO  08:43:25 Successfully provisioned 4 aws-ec2 machines
INFO  08:43:25 ==============================================================
INFO  08:43:25 Pausing for Machine Warm up... (10 sec)
INFO  08:43:36 Hazelcast Stabilizer Coordinator
INFO  08:43:36 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:43:36 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3
INFO  08:43:36 Loading stabilizer.properties: /tmp/yourproject/workdir/stabilizer.properties
INFO  08:43:36 Loading testsuite file: /tmp/yourproject/workdir/../conf/test.properties
INFO  08:43:36 Loading Hazelcast configuration: /tmp/yourproject/workdir/../conf/hazelcast.xml
INFO  08:43:36 Loading Hazelcast client configuration: /tmp/yourproject/workdir/../conf/client-hazelcast.xml
INFO  08:43:36 Loading agents file: /tmp/yourproject/workdir/agents.txt
INFO  08:43:36 --------------------------------------------------------------
INFO  08:43:36 Waiting for agents to start
INFO  08:43:36 --------------------------------------------------------------
INFO  08:43:36 Connect to agent 54.91.98.103 OK
INFO  08:43:37 Connect to agent 54.237.144.164 OK
INFO  08:43:37 Connect to agent 54.196.60.36 OK
INFO  08:43:37 Connect to agent 54.226.58.200 OK
INFO  08:43:37 --------------------------------------------------------------
INFO  08:43:37 All agents are reachable!
INFO  08:43:37 --------------------------------------------------------------
INFO  08:43:37 Performance monitor enabled: false
INFO  08:43:37 Total number of agents: 4
INFO  08:43:37 Total number of Hazelcast member workers: 2
INFO  08:43:37 Total number of Hazelcast client workers: 2
INFO  08:43:37 Total number of Hazelcast mixed client & member workers: 0
INFO  08:43:38 Copying workerClasspath '../target/*.jar' to agents
INFO  08:43:49 Finished copying workerClasspath '../target/*.jar' to agents
INFO  08:43:49 Starting workers
INFO  08:44:03 Finished starting a grand total of 4 Workers JVM's after 14463 ms
INFO  08:44:04 Starting testsuite: 1405230216265
INFO  08:44:04 Tests in testsuite: 1
INFO  08:44:05 Running time per test: 00d 00h 05m 00s 
INFO  08:44:05 Expected total testsuite time: 00d 00h 05m 00s
INFO  08:44:05 Running 1 tests sequentially
INFO  08:44:05 --------------------------------------------------------------
Running Test : 
TestCase{
      id=
    , class=yourgroupid.ExampleTest
    , logFrequency=10000
    , performanceUpdateFrequency=10000
    , threadCount=1
}
--------------------------------------------------------------
INFO  08:44:06 Starting Test initialization
INFO  08:44:07 Completed Test initialization
INFO  08:44:08 Starting Test setup
INFO  08:44:10 Completed Test setup
INFO  08:44:11 Starting Test local warmup
INFO  08:44:13 Completed Test local warmup
INFO  08:44:14 Starting Test global warmup
INFO  08:44:16 Completed Test global warmup
INFO  08:44:16 Starting Test start
INFO  08:44:18 Completed Test start
INFO  08:44:18 Test will run for 00d 00h 05m 00s
INFO  08:44:48 Running 00d 00h 00m 30s, 10.00 percent complete
INFO  08:45:18 Running 00d 00h 01m 00s, 20.00 percent complete
INFO  08:45:48 Running 00d 00h 01m 30s, 30.00 percent complete
INFO  08:46:18 Running 00d 00h 02m 00s, 40.00 percent complete
INFO  08:46:48 Running 00d 00h 02m 30s, 50.00 percent complete
INFO  08:47:18 Running 00d 00h 03m 00s, 60.00 percent complete
INFO  08:47:48 Running 00d 00h 03m 30s, 70.00 percent complete
INFO  08:48:18 Running 00d 00h 04m 00s, 80.00 percent complete
INFO  08:48:48 Running 00d 00h 04m 30s, 90.00 percent complete
INFO  08:49:18 Running 00d 00h 05m 00s, 100.00 percent complete
INFO  08:49:19 Test finished running
INFO  08:49:19 Starting Test stop
INFO  08:49:22 Completed Test stop
INFO  08:49:22 Starting Test global verify
INFO  08:49:25 Completed Test global verify
INFO  08:49:25 Starting Test local verify
INFO  08:49:28 Completed Test local verify
INFO  08:49:28 Starting Test global tear down
INFO  08:49:31 Finished Test global tear down
INFO  08:49:31 Starting Test local tear down
INFO  08:49:34 Completed Test local tear down
INFO  08:49:34 Terminating workers
INFO  08:49:35 All workers have been terminated
INFO  08:49:35 Starting cool down (10 sec)
INFO  08:49:45 Finished cool down
INFO  08:49:45 Total running time: 340 seconds
INFO  08:49:45 -----------------------------------------------------------------------------
INFO  08:49:45 No failures have been detected!
INFO  08:49:45 -----------------------------------------------------------------------------
INFO  08:49:46 Hazelcast Stabilizer Provisioner
INFO  08:49:46 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:49:46 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3
INFO  08:49:46 Loading stabilizer.properties: /tmp/yourproject/workdir/stabilizer.properties
INFO  08:49:46 ==============================================================
INFO  08:49:46 Download artifacts of 4 machines
INFO  08:49:46 ==============================================================
INFO  08:49:46 Downloading from 54.91.98.103
INFO  08:49:49 Downloading from 54.237.144.164
INFO  08:49:51 Downloading from 54.196.60.36
INFO  08:49:53 Downloading from 54.226.58.200
INFO  08:49:56 ==============================================================
INFO  08:49:56 Finished Downloading Artifacts of 4 machines
INFO  08:49:56 ==============================================================
INFO  08:49:56 Hazelcast Stabilizer Provisioner
INFO  08:49:56 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:49:56 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3
INFO  08:49:56 Loading stabilizer.properties: /tmp/yourproject/workdir/stabilizer.properties
INFO  08:49:56 ==============================================================
INFO  08:49:56 Terminating 4 aws-ec2 machines (can take some time)
INFO  08:49:56 ==============================================================
INFO  08:49:56 Current number of machines: 4
INFO  08:49:56 Desired number of machines: 0
INFO  08:50:29 	54.196.60.36 Terminating
INFO  08:50:29 	54.237.144.164 Terminating
INFO  08:50:29 	54.226.58.200 Terminating
INFO  08:50:29 	54.91.98.103 Terminating
INFO  08:51:16 Updating /tmp/yourproject/workdir/agents.txt
INFO  08:51:16 Duration: 00d 00h 01m 20s
INFO  08:51:16 ==============================================================
INFO  08:51:16 Finished terminating 4 aws-ec2 machines, 0 machines remaining.
INFO  08:51:16 ==============================================================
```