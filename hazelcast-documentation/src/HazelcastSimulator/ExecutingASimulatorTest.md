

## Executing a Simulator Test

After you install and prepare the Hazelcast Simulator for your environment, it is time to perform a test.

In the following sections, we provide an example test and its output along with the required edits to the files `simulator.properties` and `test.properties`.

### Creating and editing properties file

***NOTE:*** *For a full description of the file `simulator.properties`, please see the [Simulator.Properties File Description section](#simulator-properties-file-description). Also you can find sample simulator properties in the `dist/simulator-tests/simulator.properties`. You can copy the this file to working directory ,then you can edit according to your needs.*

You need to give the classpath of `test` in the file `test.properties` as shown below.

```
example@class=com.hazelcast.simulator.tests.ExampleTest
example@maxKeys=5000
example@putProb=0.4
```

* The `example` word at the start of the each line, represents `id` of the `test suite`. 
* The property `class` defines the actual test case and the rest are the properties you want to bind in your test. If a
property is not defined in this file, the default value of the property given in your test code is used. Please see the `properties` comment in the example code above.

You can also define multiple tests in the file `test.properties` as shown below.

```
foo@class=yourgroupid.ExampleTest
foo@maxKeys=5000

bar@class=yourgroupid.ExampleTest
bar@maxKeys=5000

```

This is useful if you want to run multiple tests sequentially, or tests in parallel using the `coordinator --parallel` option. Please see the [Coordinator section](#coordinator) for more information.
 
<br></br>
***RELATED INFORMATION***

*Please see the [Provisioner section](#provisioner) and the [Coordinator section](#coordinator) for the more `provisioner` and `coordinator` commands.*
<br></br>

### Running the Test

When you are in your working folder, execute the following commands step by step to start the test.

* First of all, you need agents to run the test on them.


	```provisioner --scale 4```
	

This line of command starts 4 EC2 instances, install Java and the agents.

The output of the command looks like the following.

```
INFO  13:25:35 Hazelcast Simulator Provisioner
INFO  13:25:35 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:25:35 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:25:35 Loading simulator.properties: /disk1/hasan/exampleSandbox/simulator.properties
INFO  13:25:36 ==============================================================
INFO  13:25:36 Provisioning 4 aws-ec2 machines
INFO  13:25:36 ==============================================================
INFO  13:25:36 Current number of machines: 0
INFO  13:25:36 Desired number of machines: 4
INFO  13:25:36 GroupName: simulator-agent-hasan
INFO  13:25:36 Username: simulator
INFO  13:25:36 Using init script:/disk1/hasan/hazelcast-simulator-0.5/conf/init.sh
INFO  13:25:36 JDK spec: oracle 7
INFO  13:25:36 Hazelcast version-spec: maven=3.5
INFO  13:25:36 Artifact: /disk1/hasan/.m2/repository/com/hazelcast/hazelcast/3.5/hazelcast-3.5.jar is not found in local maven repository, trying online one
INFO  13:25:36 Artifact: /disk1/hasan/.m2/repository/com/hazelcast/hazelcast-client/3.5/hazelcast-client-3.5.jar is not found in local maven repository, trying online one
INFO  13:25:41 Artifact: /disk1/hasan/.m2/repository/com/hazelcast/hazelcast-wm/3.5/hazelcast-wm-3.5.jar is not found in local maven repository, trying online one
INFO  13:25:50 Created compute
INFO  13:25:50 Machine spec: hardwareId=m3.medium,locationId=us-east-1,imageId=us-east-1/ami-fb8e9292
INFO  13:25:56 Created template
INFO  13:25:56 Login name to the remote machines: simulator
INFO  13:25:57 Security group: 'simulator' is found in region 'us-east-1'
INFO  13:25:57 Creating machines... (can take a few minutes)
INFO  13:27:10     54.157.207.79 LAUNCHED
INFO  13:27:10     54.205.76.229 LAUNCHED
INFO  13:27:10     54.158.159.32 LAUNCHED
INFO  13:27:10     54.157.2.215 LAUNCHED
INFO  13:27:21     54.157.207.79 JAVA INSTALLED
INFO  13:27:24     54.205.76.229 JAVA INSTALLED
INFO  13:27:27     54.158.159.32 JAVA INSTALLED
INFO  13:27:28     54.157.2.215 JAVA INSTALLED
INFO  13:27:33     54.157.207.79 SIMULATOR AGENT INSTALLED
INFO  13:27:33 Killing Agent on: 54.157.207.79
INFO  13:27:33 Starting Agent on: 54.157.207.79
INFO  13:27:33     54.157.207.79 SIMULATOR AGENT STARTED
INFO  13:27:35     54.205.76.229 SIMULATOR AGENT INSTALLED
INFO  13:27:35 Killing Agent on: 54.205.76.229
INFO  13:27:35 Starting Agent on: 54.205.76.229
INFO  13:27:35     54.205.76.229 SIMULATOR AGENT STARTED
INFO  13:27:40     54.158.159.32 SIMULATOR AGENT INSTALLED
INFO  13:27:40 Killing Agent on: 54.158.159.32
INFO  13:27:40 Starting Agent on: 54.158.159.32
INFO  13:27:40     54.158.159.32 SIMULATOR AGENT STARTED
INFO  13:27:46     54.157.2.215 SIMULATOR AGENT INSTALLED
INFO  13:27:46 Killing Agent on: 54.157.2.215
INFO  13:27:50 Starting Agent on: 54.157.2.215
INFO  13:27:51     54.157.2.215 SIMULATOR AGENT STARTED
INFO  13:27:51 Pausing for machine warmup... (10 sec)
INFO  13:28:01 Duration: 00d 00h 02m 24s
INFO  13:28:01 ==============================================================
INFO  13:28:01 Successfully provisioned 4 aws-ec2 machines
INFO  13:28:01 ==============================================================
INFO  13:28:01 Shutting down Provisioner...
INFO  13:28:01 Done!
```
Also you can see `agents.txt` was created automatically by provisioner in the working directory. This `agents.txt` file includes ips of the EC2 instances.

* After the create instances and install agents to them. You need cordinate and configure your test suite via `cooordinator`.
	
	```
	coordinator     --memberWorkerCount 2 \
                --workerVmOptions "-ea -server -Xms2G -Xmx2G -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -XX:+HeapDumpOnOutOfMemoryError" \
                --hzFile            hazelcast.xml \
                --clientWorkerCount 2 \
                --clientWorkerVmOptions "-ea -server -Xms2G -Xmx2G -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -XX:+HeapDumpOnOutOfMemoryError" \
                --clientHzFile      client-hazelcast.xml \
                --duration          5m \
                --monitorPerformance \
                test.properties
	```

Please see the [Coordinator section](#coordinator) for detailed information about coordinator arguments.

The output of the command looks like the following.

```
INFO  13:28:52 Hazelcast Simulator Coordinator
INFO  13:28:52 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:28:52 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:28:52 Loading simulator.properties: /disk1/hasan/exampleSandbox/simulator.properties
INFO  13:28:52 Loading testsuite file: /disk1/hasan/exampleSandbox/test.properties
INFO  13:28:52 Loading Hazelcast configuration: /disk1/hasan/exampleSandbox/hazelcast.xml
INFO  13:28:52 Loading Hazelcast client configuration: /disk1/hasan/exampleSandbox/client-hazelcast.xml
INFO  13:28:52 Loading Log4j configuration for worker: /disk1/hasan/hazelcast-simulator-0.5/conf/worker-log4j.xml
INFO  13:28:52 Loading agents file: /disk1/hasan/exampleSandbox/agents.txt
INFO  13:28:52 HAZELCAST_VERSION_SPEC: maven=3.5
INFO  13:28:52 --------------------------------------------------------------
INFO  13:28:52 Waiting for agents to start
INFO  13:28:52 --------------------------------------------------------------
INFO  13:28:52 Connect to agent 54.157.207.79 OK
INFO  13:28:52 Connect to agent 54.205.76.229 OK
INFO  13:28:53 Connect to agent 54.158.159.32 OK
INFO  13:28:53 Connect to agent 54.157.2.215 OK
INFO  13:28:53 --------------------------------------------------------------
INFO  13:28:53 All agents are reachable!
INFO  13:28:53 --------------------------------------------------------------
INFO  13:28:53 Performance monitor enabled: true
INFO  13:28:53 Total number of agents: 4
INFO  13:28:53 Total number of Hazelcast member workers: 2
INFO  13:28:53 Total number of Hazelcast client workers: 2
INFO  13:28:53     Agent 54.157.207.79 members: 1 clients: 0 mode: MIXED
INFO  13:28:53     Agent 54.205.76.229 members: 1 clients: 0 mode: MIXED
INFO  13:28:53     Agent 54.158.159.32 members: 0 clients: 1 mode: MIXED
INFO  13:28:53     Agent 54.157.2.215 members: 0 clients: 1 mode: MIXED
INFO  13:28:53 Killing all remaining workers
INFO  13:28:53 Successfully killed all remaining workers
INFO  13:28:53 Starting 2 member workers
INFO  13:29:12 Successfully started member workers
INFO  13:29:12 Starting 2 client workers
INFO  13:29:16 Successfully started client workers
INFO  13:29:16 Successfully started a grand total of 4 Workers JVMs after 22259 ms
INFO  13:29:16 Starting testsuite: 2015-07-01__13_28_52
INFO  13:29:16 Tests in testsuite: 1
INFO  13:29:16 Running time per test: 00d 00h 05m 00s
INFO  13:29:16 Expected total testsuite time: 00d 00h 05m 00s
INFO  13:29:16 Running 1 tests sequentially
INFO  13:29:16 --------------------------------------------------------------
Running Test: example
TestCase{
      id=example
    , class=com.hazelcast.simulator.tests.ExampleTest
    , maxKeys=5000
    , putProb=0.4
}
--------------------------------------------------------------
INFO  13:29:16 example Starting Test initialization
INFO  13:29:16 example Completed Test initialization
INFO  13:29:16 example Starting Test setup
INFO  13:29:18 example Completed Test setup
INFO  13:29:18 example Starting Test local warmup
INFO  13:29:20 example Completed Test local warmup
INFO  13:29:20 example Starting Test global warmup
INFO  13:29:22 example Completed Test global warmup
INFO  13:29:22 example Starting Test start
INFO  13:29:23 example Completed Test start
INFO  13:29:23 example Test will run for 00d 00h 05m 00s
INFO  13:29:53 example Running 00d 00h 00m 30s   10.00% complete         71,586 ops        4,851.09 ops/s
INFO  13:30:23 example Running 00d 00h 01m 00s   20.00% complete        311,699 ops        7,803.53 ops/s
INFO  13:30:53 example Running 00d 00h 01m 30s   30.00% complete        573,207 ops        7,691.47 ops/s
INFO  13:31:23 example Running 00d 00h 02m 00s   40.00% complete        832,858 ops        8,141.40 ops/s
INFO  13:31:53 example Running 00d 00h 02m 30s   50.00% complete      1,099,715 ops        8,503.92 ops/s
INFO  13:32:23 example Running 00d 00h 03m 00s   60.00% complete      1,275,027 ops        7,768.38 ops/s
INFO  13:32:53 example Running 00d 00h 03m 30s   70.00% complete      1,542,956 ops        8,612.40 ops/s
INFO  13:33:23 example Running 00d 00h 04m 00s   80.00% complete      1,806,133 ops        8,249.75 ops/s
INFO  13:33:53 example Running 00d 00h 04m 30s   90.00% complete      2,072,388 ops        8,442.62 ops/s
INFO  13:34:23 example Running 00d 00h 05m 00s  100.00% complete      2,244,486 ops        7,519.94 ops/s
INFO  13:34:23 example Test finished running
INFO  13:34:23 example Starting Test stop
INFO  13:34:25 example Completed Test stop
INFO  13:34:25 Total performance        100.00%       2,334,730 ops        7,782.43 ops/s
INFO  13:34:25   Agent 54.157.207.79      0.00%               0 ops            0.00 ops/s
INFO  13:34:25   Agent 54.157.2.215      50.63%       1,182,008 ops        3,940.03 ops/s
INFO  13:34:25   Agent 54.205.76.229      0.00%               0 ops            0.00 ops/s
INFO  13:34:25   Agent 54.158.159.32     49.37%       1,152,722 ops        3,842.41 ops/s
INFO  13:34:26 example Starting Test global verify
INFO  13:34:28 example Waiting for globalVerify completion: 00d 00h 00m 01s
INFO  13:34:34 example Completed Test global verify
INFO  13:34:34 example Starting Test local verify
INFO  13:34:36 example Completed Test local verify
INFO  13:34:36 example Starting Test global tear down
INFO  13:34:37 example Finished Test global tear down
INFO  13:34:37 example Starting Test local tear down
INFO  13:34:39 example Completed Test local tear down
INFO  13:34:39 Terminating workers
INFO  13:34:39 All workers have been terminated
INFO  13:34:39 Starting cool down (10 sec)
INFO  13:34:49 Finished cool down
INFO  13:34:49 Total running time: 333 seconds
INFO  13:34:49 -----------------------------------------------------------------------------
INFO  13:34:49 No failures have been detected!
INFO  13:34:49 -----------------------------------------------------------------------------
```


* Now you need logs and results that produced by workers. You can get these requirements from agents via `provisioner`.  

	``` provisioner --download ```

This line performs the following.

```
INFO  13:36:40 Hazelcast Simulator Provisioner
INFO  13:36:40 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:36:40 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:36:40 Loading simulator.properties: /disk1/hasan/exampleSandbox/simulator.properties
INFO  13:36:41 ==============================================================
INFO  13:36:41 Download artifacts of 4 machines
INFO  13:36:41 ==============================================================
INFO  13:36:41 Downloading from 54.157.207.79
INFO  13:36:41 Downloading from 54.205.76.229
INFO  13:36:41 Downloading from 54.158.159.32
INFO  13:36:41 Downloading from 54.157.2.215
INFO  13:36:41 ==============================================================
INFO  13:36:41 Finished Downloading Artifacts of 4 machines
INFO  13:36:41 ==============================================================
INFO  13:36:41 Shutting down Provisioner...
INFO  13:36:41 Done!
```
 
 After it is completed, the artifacts (log files) are downloaded into the `workers` folder in the working directory.


* If want to terminate the instances, just run this line of command.

	```provisioner --terminate```

This line performs the following.

```
INFO  13:38:28 Hazelcast Simulator Provisioner
INFO  13:38:28 Version: 0.5, Commit: c6e82c5, Build Time: 18.06.2015 @ 11:58:06 UTC
INFO  13:38:28 SIMULATOR_HOME: /disk1/hasan/hazelcast-simulator-0.5
INFO  13:38:28 Loading simulator.properties: /disk1/hasan/exampleSandbox/simulator.properties
INFO  13:38:29 ==============================================================
INFO  13:38:29 Terminating 4 aws-ec2 machines (can take some time)
INFO  13:38:29 ==============================================================
INFO  13:38:29 Current number of machines: 4
INFO  13:38:29 Desired number of machines: 0
INFO  13:38:51     54.205.76.229 Terminating
INFO  13:38:51     54.157.2.215 Terminating
INFO  13:38:51     54.157.207.79 Terminating
INFO  13:38:51     54.158.159.32 Terminating
INFO  13:39:54 Updating /disk1/hasan/exampleSandbox/agents.txt
INFO  13:39:54 Duration: 00d 00h 01m 25s
INFO  13:39:54 ==============================================================
INFO  13:39:54 Terminated 4 of 4, remaining=0
INFO  13:39:54 ==============================================================
INFO  13:39:54 Shutting down Provisioner...
INFO  13:39:54 Done!
```

Alternative way to run test via script. The script `run.sh` is for your convenience. It gathers all the commands used to perform a test into one script. The following is the content of this example `run.sh` script.

```
#!/bin/bash

set -e

provisioner --scale 4

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
