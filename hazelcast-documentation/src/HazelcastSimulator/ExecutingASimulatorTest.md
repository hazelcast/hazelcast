

## Executing a Simulator Test

After you install and prepare the Hazelcast Simulator for your environment, it is time to perform a test.

The following steps wrap up the whole procedure for executing a Hazelcast Simulator test.

1. Install the Hazelcast Simulator.
2. Create a directory for your tests, let's call it as the working directory.
3. Copy the `simulator.properties` file from the `/conf` directory of Hazelcast Simulator to your working directory.
4. Edit the `simulator.properties` file according to your needs.
5. Copy the `test.properties` file from the `/simulator-tests` directory of Hazelcast Simulator to your working directory.
6. Edit the `test.properties` file according to your needs.
5. Execute the `run.sh` script while you are in your working directory to perform your Simulator test.

In the following sections, we provide an example test and its output along with the required file (`simulator.properties` and `test.properties`) edits.

### An Example Simulator Test

The following example test performs `put` and `get` operations on a Hazelcast Map and verifies the key-value ownership, and also prints the size of the map.

```
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static junit.framework.TestCase.assertEquals;

public class ExampleTest {

    private enum Operation {
        PUT,
        GET
    }

    private static final ILogger log = Logger.getLogger(ExampleTest.class);

    //properties
    public double putProb = 0.5;
    public int maxKeys = 1000;

    private TestContext testContext;
    private IMap map;

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        log.info("======== SETUP =========");
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap("exampleMap");

        log.info("Map name is:" + map.getName());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Warmup
    public void warmup() {
        log.info("======== WARMUP =========");
        log.info("Map size is:" + map.size());
    }

    @Verify
    public void verify() {
        log.info("======== VERIFYING =========");
        log.info("Map size is:" + map.size());

        for (int i = 0; i < maxKeys; i++) {
            assertEquals(map.get(i), "value" + i);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        log.info("======== TEAR DOWN =========");
        map.destroy();
        log.info("======== THE END =========");
    }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) {
            int key = randomInt(maxKeys);
            switch (operation) {
                case PUT:
                    map.put(key, "value" + key);
                    break;
                case GET:
                    map.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation" + operation);
            }
        }

    }

    public static void main(String[] args) throws Throwable {
        ExampleTest test = new ExampleTest();
        new TestRunner<ExampleTest>(test).run();
    }
}
```


### Editing `Simulator.Properties`

 
In the case of Amazon EC2, you need to consider the following properties.

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

Create two text files in your home directory. The file `ec2.identity` should contain your access key and the file 
`ec2.credential` should contain your secret key. 

***NOTE:*** *For a full description of the file `simulator.properties`, please see the [Simulator.Properties File Description section](#simulator-properties-file-description).*

### Editing `test.properties`

You need to give the classpath of `Example` test in the file `test.properties` as shown below.


```
class=yourgroupid.ExampleTest
maxKeys=5000
putProb=0.4
```

The property `class` defines the actual test case and the rest are the properties you want to bind in your test. If a
property is not defined in this file, the default value of the property given in your test code is used. Please see the `properties` comment in the example code above.

You can also define multiple tests in the file `test.properties` as shown below.

```
foo.class=yourgroupid.ExampleTest
foo.maxKeys=5000

bar.class=yourgroupid.ExampleTest
bar.maxKeys=5000

```

This is useful if you want to run multiple tests sequentially, or tests in parallel using the `coordinator --parallel` option. Please see the [Coordinator section](#coordinator) for more information.

### Running the Test

When you are in your working directory, execute the following command to start the test.


```
./run.sh
```

The script `run.sh` is for your convenience which gathers all commands used to perform a test in one script. The following is the content of this example `run.sh` script.

```
#!/bin/bash

set -e

coordinator     --memberWorkerCount 2 \
                --workerVmOptions "-ea -server -Xms2G -Xmx2G -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -XX:+HeapDumpOnOutOfMemoryError" \
                --hzFile            hazelcast.xml \
                --clientWorkerCount 2 \
                --clientWorkerVmOptions "-ea -server -Xms2G -Xmx2G -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -XX:+HeapDumpOnOutOfMemoryError" \
                --clientHzFile      client-hazelcast.xml \
                --workerClassPath   '../target/*.jar' \
                --duration          5m \
                --monitorPerformance \
                test.properties

provisioner --download
```

This script performs the following.

 * Start 4 EC2 instances, install Java and the agents.
 * Upload your JARs, run the test using a 2 node test cluster and 2 client machines (the clients generate the load).
 
 
This test runs for 2 minutes. After it is completed, the artifacts (log files) are downloaded in the `workers` directory. Then, it terminates the 4 instances. If you do not want to start/terminate the instances for every run, just comment out the line `provisioner --terminate` in the script `run.sh`. This prevents the machines from being terminated. Please see the [Provisioner section](#provisioner) for more information.

<br></br>
***RELATED INFORMATION***

*Please see the [Provisioner section](#provisioner) and the [Coordinator section](#coordinator) for the `provisioner` and `coordinator` commands you see in the script `run.sh`.*
<br></br>

The output of the test looks like the following.

```
INFO  08:40:10 Hazelcast Simulator Provisioner
INFO  08:40:10 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:40:10 SIMULATOR_HOME: /home/alarmnummer/hazelcast-simulator-0.3
INFO  08:40:10 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
INFO  08:40:10 ==============================================================
INFO  08:40:10 Provisioning 4 aws-ec2 machines
INFO  08:40:10 ==============================================================
INFO  08:40:10 Current number of machines: 0
INFO  08:40:10 Desired number of machines: 4
INFO  08:40:10 GroupName: simulator-agent
INFO  08:40:10 JDK spec: oracle 7
INFO  08:40:10 Hazelcast version-spec: outofthebox
INFO  08:40:12 Created compute
INFO  08:40:12 Machine spec: hardwareId=m3.medium,locationId=us-east-1,imageId=us-east-1/ami-fb8e9292
INFO  08:40:19 Security group: 'simulator' is found in region 'us-east-1'
INFO  08:40:27 Created template
INFO  08:40:27 Loginname to the remote machines: simulator
INFO  08:40:27 Creating machines (can take a few minutes)
INFO  08:42:10 	54.91.98.103 LAUNCHED
INFO  08:42:10 	54.237.144.164 LAUNCHED
INFO  08:42:10 	54.196.60.36 LAUNCHED
INFO  08:42:10 	54.226.58.200 LAUNCHED
INFO  08:42:24 	54.196.60.36 JAVA INSTALLED
INFO  08:42:25 	54.237.144.164 JAVA INSTALLED
INFO  08:42:27 	54.91.98.103 JAVA INSTALLED
INFO  08:42:30 	54.226.58.200 JAVA INSTALLED
INFO  08:42:57 	54.196.60.36 SIMULATOR AGENT INSTALLED
INFO  08:42:59 	54.237.144.164 SIMULATOR AGENT INSTALLED
INFO  08:43:01 	54.196.60.36 SIMULATOR AGENT STARTED
INFO  08:43:02 	54.237.144.164 SIMULATOR AGENT STARTED
INFO  08:43:06 	54.91.98.103 SIMULATOR AGENT INSTALLED
INFO  08:43:09 	54.91.98.103 SIMULATOR AGENT STARTED
INFO  08:43:21 	54.226.58.200 SIMULATOR AGENT INSTALLED
INFO  08:43:25 	54.226.58.200 SIMULATOR AGENT STARTED
INFO  08:43:25 Duration: 00d 00h 03m 15s
INFO  08:43:25 ==============================================================
INFO  08:43:25 Successfully provisioned 4 aws-ec2 machines
INFO  08:43:25 ==============================================================
INFO  08:43:25 Pausing for Machine Warm up... (10 sec)
INFO  08:43:36 Hazelcast Simulator Coordinator
INFO  08:43:36 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:43:36 SIMULATOR_HOME: /home/alarmnummer/hazelcast-simulator-0.3
INFO  08:43:36 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
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
    , maxKeys=5000
    , putProb=0.4
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
INFO  08:49:46 Hazelcast Simulator Provisioner
INFO  08:49:46 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:49:46 SIMULATOR_HOME: /home/alarmnummer/hazelcast-simulator-0.3
INFO  08:49:46 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
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
INFO  08:49:56 Hazelcast Simulator Provisioner
INFO  08:49:56 Version: 0.3, Commit: 2af49f0, Build Time: 13.07.2014 @ 08:37:06 EEST
INFO  08:49:56 SIMULATOR_HOME: /home/alarmnummer/hazelcast-simulator-0.3
INFO  08:49:56 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
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

### Using Maven Archetypes

Alternatively, you can execute tests using the Simulator archetype. Please see the following:

```
mvn archetype:generate  \
    -DarchetypeGroupId=com.hazelcast.simulator \
    -DarchetypeArtifactId=archetype \
    -DarchetypeVersion=0.3 \
    -DgroupId=yourgroupid  \
    -DartifactId=yourproject
```

This will create fully working Simulator project including the test having `yourgroupid`. 

1. After this project is generated, go to the created directory and run the following command.

   ```
mvn clean install
   ```

2. Then, go to your working directory.
 
   ```
cd <working directory>
   ```
 
3. Edit the `simulator.properties` file as explained in the [Editing the Simulator.Properties File section](#editing-the-simulator-properties-file). 

4. Run the test from your working directory using the following command.

   ```
./run.sh
   ```

The output is the same as shown in the [Running the Test section](#running-the-test).