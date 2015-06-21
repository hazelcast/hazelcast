

## Executing a Simulator Test

After you install and prepare the Hazelcast Simulator for your environment, it is time to perform a test.

The following steps execute a Hazelcast Simulator test.

1. Install the Hazelcast Simulator.
2. Create a folder85 for your tests. Let's call it your working folder.
3. Copy the `simulator.properties` file from the `/conf` folder of the Hazelcast Simulator to your working folder.
4. Edit the `simulator.properties` file according to your needs.
5. Copy the `test.properties` file from the `/simulator-tests` folder of Hazelcast Simulator to your working folder.
6. Edit the `test.properties` file according to your needs.
5. Execute the `run.sh` script while you are in your working folder to perform your Simulator test.

In the following sections, we provide an example test and its output along with the required edits to the files `simulator.properties` and `test.properties`.

### An Example Simulator Test

The following example code performs `put` and `get` operations on a Hazelcast Map and verifies the key-value ownership, and it also prints the size of the map.

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


### Editing the simulator.properties File

 
In the case of Amazon EC2, you need to consider the following properties.

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

Create two text files in your home folder. The file `ec2.identity` should contain your access key and the file 
`ec2.credential` should contain your secret key. 

***NOTE:*** *For a full description of the file `simulator.properties`, please see the [Simulator.Properties File Description section](#simulator-properties-file-description).*

### Editing the test.properties file

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

When you are in your working folder, execute the following command to start the test.

Firstly, you should add simulator worker directory path to bash as an enviroment variable:
```
export SIMULATOR_HOME=~/hazelcast-simulator-0.5
PATH=$SIMULATOR_HOME/bin:$PATH
```
Then run the script:
```
./run.sh
```

The script `run.sh` is for your convenience. It gathers all the commands used to perform a test into one script. The following is the content of this example `run.sh` script.

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
 * Upload your JARs, run the test using a 2 node test cluster(--memberWorkerCount 2) and 2 client machines (--clientWorkerCount 2) (the clients generate the load).
 * --workerClassPath argument takes classpath of file/directory containing the classes/jars/resources that are going to be uploaded to the agents.
 * --monitorPerformance provides get some performance numbers from suite like this:
 ```
62,299 ops        3,970.69 ops/s
304,306 ops        7,767.17 ops/s
 ```
 
 
This test runs for 2 minutes. After it is completed, the artifacts (log files) are downloaded into the `workers` folder. Then, it terminates the 4 instances. If you do not want to start/terminate the instances for every run, just comment out the line `provisioner --terminate` in the script `run.sh`. This prevents the machines from being terminated. Please see the [Provisioner section](#provisioner) for more information.

<br></br>
***RELATED INFORMATION***

*Please see the [Provisioner section](#provisioner) and the [Coordinator section](#coordinator) for the `provisioner` and `coordinator` commands you see in the script `run.sh`.*
<br></br>

The output of the test looks like the following.

```
INFO  13:46:12 Hazelcast Simulator Provisioner
INFO  13:46:12 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:46:12 SIMULATOR_HOME: .../hasan/hazelcast-simulator-0.5
INFO  13:46:12 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
INFO  13:46:13 ==============================================================
INFO  13:46:13 Provisioning 4 aws-ec2 machines
INFO  13:46:13 ==============================================================
INFO  13:46:13 Current number of machines: 0
INFO  13:46:13 Desired number of machines: 4
INFO  13:46:13 Username: simulator
INFO  13:46:13 Using init script:/disk1/hasan/hazelcast-simulator-0.5/conf/init.sh
INFO  13:46:13 JDK spec: oracle 7
INFO  13:46:13 Hazelcast version-spec: maven=3.5 (or outofthebox)
INFO  13:46:13 Artifact: ../.m2/repository/com/hazelcast/hazelcast/3.5/hazelcast-3.5.jar is not found in local maven repository, trying online one
INFO  13:46:13 Artifact: ../.m2/repository/com/hazelcast/hazelcast-client/3.5/hazelcast-client-3.5.jar is not found in local maven repository, trying online one
INFO  13:46:18 Artifact: ../.m2/repository/com/hazelcast/hazelcast-wm/3.5/hazelcast-wm-3.5.jar is not found in local maven repository, trying online one
INFO  13:46:26 Created compute
INFO  13:46:26 Machine spec: hardwareId=m3.medium,locationId=us-east-1,imageId=us-east-1/ami-fb8e9292
INFO  13:46:32 Created template
INFO  13:46:32 Login name to the remote machines: simulator
INFO  13:46:32 Security group: 'simulator' is found in region 'us-east-1'
INFO  13:46:32 Creating machines... (can take a few minutes)
INFO  13:47:28     54.237.60.217 LAUNCHED
INFO  13:47:28     54.237.88.144 LAUNCHED
INFO  13:47:28     54.157.129.67 LAUNCHED
INFO  13:47:28     54.237.126.248 LAUNCHED
INFO  13:47:38     54.237.60.217 JAVA INSTALLED
INFO  13:47:39     54.237.88.144 JAVA INSTALLED
INFO  13:47:42     54.237.126.248 JAVA INSTALLED
INFO  13:47:43     54.157.129.67 JAVA INSTALLED
INFO  13:47:52     54.237.60.217 SIMULATOR AGENT INSTALLED
INFO  13:47:52 Killing Agent on: 54.237.60.217
INFO  13:47:52 Starting Agent on: 54.237.60.217
INFO  13:47:52     54.237.60.217 SIMULATOR AGENT STARTED
INFO  13:47:53     54.237.88.144 SIMULATOR AGENT INSTALLED
INFO  13:47:53 Killing Agent on: 54.237.88.144
INFO  13:47:53 Starting Agent on: 54.237.88.144
INFO  13:47:53     54.237.88.144 SIMULATOR AGENT STARTED
INFO  13:47:53     54.237.126.248 SIMULATOR AGENT INSTALLED
INFO  13:47:53 Killing Agent on: 54.237.126.248
INFO  13:47:53 Starting Agent on: 54.237.126.248
INFO  13:47:53     54.237.126.248 SIMULATOR AGENT STARTED
INFO  13:47:54     54.157.129.67 SIMULATOR AGENT INSTALLED
INFO  13:47:54 Killing Agent on: 54.157.129.67
INFO  13:47:54 Starting Agent on: 54.157.129.67
INFO  13:47:54     54.157.129.67 SIMULATOR AGENT STARTED
INFO  13:47:54 Pausing for machine warmup... (10 sec)
INFO  13:48:04 Duration: 00d 00h 01m 51s
INFO  13:48:04 ==============================================================
INFO  13:48:04 Successfully provisioned 4 aws-ec2 machines
INFO  13:48:04 ==============================================================
INFO  13:48:04 Shutting down Provisioner...
INFO  13:48:04 Done!
INFO  13:48:04 Hazelcast Simulator Coordinator
INFO  13:48:04 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:48:04 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:48:04 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties                  
INFO  13:48:04 Loading testsuite file: /tmp/yourproject/workdir/../conf/test.properties                     
INFO  13:48:04 Loading Hazelcast configuration: /tmp/yourproject/workdir/../conf/hazelcast.xml              
INFO  13:48:04 Loading Hazelcast client configuration: /tmp/yourproject/workdir/../conf/client-hazelcast.xml
INFO  13:48:04 Loading agents file: /tmp/yourproject/workdir/agents.txt                                     
INFO  13:48:04 HAZELCAST_VERSION_SPEC: maven=3.5
INFO  13:48:04 --------------------------------------------------------------
INFO  13:48:04 Waiting for agents to start
INFO  13:48:04 --------------------------------------------------------------
INFO  13:48:04 Connect to agent 54.237.60.217 OK
INFO  13:48:05 Connect to agent 54.237.88.144 OK
INFO  13:48:05 Connect to agent 54.157.129.67 OK
INFO  13:48:05 Connect to agent 54.237.126.248 OK
INFO  13:48:05 --------------------------------------------------------------
INFO  13:48:05 All agents are reachable!
INFO  13:48:05 --------------------------------------------------------------
INFO  13:48:05 Performance monitor enabled: true
INFO  13:48:05 Total number of agents: 4
INFO  13:48:05 Total number of Hazelcast member workers: 2
INFO  13:48:05 Total number of Hazelcast client workers: 2
INFO  13:48:05     Agent 54.237.60.217 members: 1 clients: 0 mode: MIXED
INFO  13:48:05     Agent 54.237.88.144 members: 1 clients: 0 mode: MIXED
INFO  13:48:05     Agent 54.157.129.67 members: 0 clients: 1 mode: MIXED
INFO  13:48:05     Agent 54.237.126.248 members: 0 clients: 1 mode: MIXED
INFO  13:48:05 Killing all remaining workers
INFO  13:48:05 Successfully killed all remaining workers
INFO  13:48:05 Starting 2 member workers
INFO  13:48:26 Successfully started member workers
INFO  13:48:26 Starting 2 client workers
INFO  13:48:30 Successfully started client workers
INFO  13:48:30 Successfully started a grand total of 4 Workers JVMs after 24470 ms
INFO  13:48:30 Starting testsuite: 2015-06-18__13_48_04
INFO  13:48:30 Tests in testsuite: 1
INFO  13:48:30 Running time per test: 00d 00h 05m 00s
INFO  13:48:30 Expected total testsuite time: 00d 00h 05m 00s
INFO  13:48:30 Running 1 tests sequentially
INFO  13:48:30 --------------------------------------------------------------
Running Test: example
TestCase{
      id=
    , class=yourgroupid.ExampleTest
    , maxKeys=5000
    , putProb=0.4
}
--------------------------------------------------------------
INFO  13:48:30 example Starting Test initialization
INFO  13:48:31 example Completed Test initialization
INFO  13:48:31 example Starting Test setup
INFO  13:48:33 example Waiting for setUp completion: 00d 00h 00m 00s
INFO  13:48:39 example Completed Test setup
INFO  13:48:39 example Starting Test local warmup
INFO  13:48:41 example Completed Test local warmup
INFO  13:48:41 example Starting Test global warmup
INFO  13:48:43 example Completed Test global warmup
INFO  13:48:43 example Starting Test start
INFO  13:48:44 example Completed Test start
INFO  13:48:44 example Test will run for 00d 00h 05m 00s
INFO  13:49:14 example Running 00d 00h 00m 30s   10.00% complete         62,299 ops        3,970.69 ops/s
INFO  13:49:44 example Running 00d 00h 01m 00s   20.00% complete        304,306 ops        7,767.17 ops/s
INFO  13:50:14 example Running 00d 00h 01m 30s   30.00% complete        572,699 ops        8,978.22 ops/s
INFO  13:50:44 example Running 00d 00h 02m 00s   40.00% complete        838,804 ops        9,087.42 ops/s
INFO  13:51:14 example Running 00d 00h 02m 30s   50.00% complete      1,098,687 ops        7,745.47 ops/s
INFO  13:51:44 example Running 00d 00h 03m 00s   60.00% complete      1,276,840 ops        8,807.83 ops/s
INFO  13:52:14 example Running 00d 00h 03m 30s   70.00% complete      1,543,390 ops        8,702.98 ops/s
INFO  13:52:44 example Running 00d 00h 04m 00s   80.00% complete      1,809,894 ops        7,789.35 ops/s
INFO  13:53:14 example Running 00d 00h 04m 30s   90.00% complete      2,076,566 ops        8,077.07 ops/s
INFO  13:53:44 example Running 00d 00h 05m 00s  100.00% complete      2,347,663 ops        8,575.74 ops/s
INFO  13:53:44 example Test finished running
INFO  13:53:44 example Starting Test stop
INFO  13:53:46 example Completed Test stop
INFO  13:53:46 Total performance        100.00%       2,347,663 ops        7,825.54 ops/s
INFO  13:53:46   Agent 54.157.129.67     50.11%       1,176,420 ops        3,921.40 ops/s
INFO  13:53:46   Agent 54.237.126.248    49.89%       1,171,243 ops        3,904.14 ops/s
INFO  13:53:46   Agent 54.237.60.217      0.00%               0 ops            0.00 ops/s
INFO  13:53:46   Agent 54.237.88.144      0.00%               0 ops            0.00 ops/s
INFO  13:53:47 example Starting Test global verify
INFO  13:53:48 example Waiting for globalVerify completion: 00d 00h 00m 01s
INFO  13:53:54 example Completed Test global verify
INFO  13:53:54 example Starting Test local verify
INFO  13:53:56 example Completed Test local verify
INFO  13:53:56 example Starting Test global tear down
INFO  13:53:57 example Finished Test global tear down
INFO  13:53:57 example Starting Test local tear down
INFO  13:53:59 example Completed Test local tear down
INFO  13:53:59 Terminating workers
INFO  13:53:59 All workers have been terminated
INFO  13:53:59 Starting cool down (10 sec)
INFO  13:54:09 Finished cool down
INFO  13:54:09 Total running time: 339 seconds
INFO  13:54:09 -----------------------------------------------------------------------------
INFO  13:54:09 No failures have been detected!
INFO  13:54:09 -----------------------------------------------------------------------------
INFO  13:54:10 Hazelcast Simulator Provisioner
INFO  13:54:10 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:54:10 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:54:10 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
INFO  13:54:10 ==============================================================
INFO  13:54:10 Download artifacts of 4 machines
INFO  13:54:10 ==============================================================
INFO  13:54:10 Downloading from 54.237.60.217
INFO  13:54:10 Downloading from 54.237.88.144
INFO  13:54:10 Downloading from 54.157.129.67
INFO  13:54:11 Downloading from 54.237.126.248
INFO  13:54:11 ==============================================================
INFO  13:54:11 Finished Downloading Artifacts of 4 machines
INFO  13:54:11 ==============================================================
INFO  13:54:11 Shutting down Provisioner...
INFO  13:54:11 Done!
INFO  13:54:11 Hazelcast Simulator Provisioner
INFO  13:54:11 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:54:11 SIMULATOR_HOME: .../hasan/hazelcast-simulator-0.5
INFO  13:54:11 Loading simulator.properties: /tmp/yourproject/workdir/simulator.properties
INFO  13:54:12 ==============================================================
INFO  13:54:12 Terminating 4 aws-ec2 machines (can take some time)
INFO  13:54:12 ==============================================================
INFO  13:54:12 Current number of machines: 4
INFO  13:54:12 Desired number of machines: 0
INFO  13:54:35     54.237.88.144 Terminating
INFO  13:54:35     54.157.129.67 Terminating
INFO  13:54:35     54.237.60.217 Terminating
INFO  13:54:35     54.237.126.248 Terminating
INFO  13:55:40 Updating /tmp/yourproject/workdir/agents.txt
INFO  13:55:40 Duration: 00d 00h 01m 28s
INFO  13:55:40 ==============================================================
INFO  13:55:40 Terminated 4 of 4, remaining=0
INFO  13:55:40 ==============================================================
INFO  13:55:40 Shutting down Provisioner...
INFO  13:55:40 Done!
```

### Using Maven Archetypes

Alternatively, you can execute tests using the Simulator archetype. Please see the following:

```
mvn archetype:generate  \
    -DarchetypeGroupId=com.hazelcast.simulator \
    -DarchetypeArtifactId=archetype \
    -DarchetypeVersion=0.5 \
    -DgroupId=yourgroupid  \
    -DartifactId=yourproject
```

This will create a fully working Simulator project, including the test having `yourgroupid`. 

1. After this project is generated, go to the created folder and run the following command.

   ```
mvn clean install
   ```

2. Then, go to your working folder.
 
   ```
cd <working folder>
   ```
 
3. Edit the `simulator.properties` file as explained in the [Editing the Simulator.Properties File section](#editing-the-simulator-properties-file). 

4. Run the test from your working folder using the following command.

   ```
./run.sh
   ```

The output is the same as shown in the [Running the Test section](#running-the-test).
