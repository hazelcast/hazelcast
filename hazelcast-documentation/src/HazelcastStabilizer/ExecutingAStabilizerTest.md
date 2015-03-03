

## Executing a Stabilizer Test

After you install and prepare the Hazelcast Stabilizer for your environment, it is time to perform a test.

The following steps wrap up the whole procedure for executing a Stabilizer Test.

1. Install the Hazelcast Stabilizer.
2. Create a directory for your tests, let's call it as the working directory.
3. Copy the `stabilizer.properties` file from the `/conf` directory of Hazelcast Stabilizer to your working directory.
4. Edit the `stabilizer.properties` file according to your needs.
5. Copy the `test.properties` file from the `/stabilizer-tests` directory of Hazelcast Stabilizer to your working directory.
6. Edit the `test.properties` file according to your needs.
5. Execute the `run.sh` script while you are in your working directory to perform your Stabilizer test.

In the following sections, we provide an example test and its output along with the required file (`stabilizer.properties` and `test.properties`) edits.

### An Example Stabilizer Test

The following example is a test where a counter is being incremented. When the test is completed, a verification is done if the actual number of increments is equal to the expected number of increments.

```
package yourGroupId;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.selector.OperationSelector;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class ExampleTest {

    private enum Operation {
        PUT,
        GET
    }

    private final static ILogger log = Logger.getLogger(ExampleTest.class);

    // properties
    public int threadCount = 1;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public double putProb = 0.2;

    // probes
    public IntervalProbe putLatencyProbe;
    public IntervalProbe getLatencyProbe;

    private IAtomicLong totalCounter;
    private AtomicLong operations = new AtomicLong();
    private IAtomicLong counter;
    private TestContext testContext;

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong("totalCounter");
        counter = targetInstance.getAtomicLong("counter");

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        long expected = totalCounter.get();
        long actual = counter.get();

        assertEquals(expected, actual);
    }

    @Teardown
    public void teardown() throws Exception {
        counter.destroy();
        totalCounter.destroy();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final OperationSelector<Operation> selector = operationSelectorBuilder.build();

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                Operation operation = selector.select();
                switch (operation) {
                    case PUT:
                        putLatencyProbe.started();
                        counter.incrementAndGet();
                        putLatencyProbe.done();
                        break;
                    case GET:
                        getLatencyProbe.started();
                        counter.get();
                        getLatencyProbe.done();
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown operation" + operation);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            operations.addAndGet(iteration % performanceUpdateFrequency);
            totalCounter.addAndGet(iteration);
        }
    }

    public static void main(String[] args) throws Throwable {
        ExampleTest test = new ExampleTest();
        new TestRunner<ExampleTest>(test).run();
    }
}
```


### Editing `Stabilizer.Properties`

 
In the case of Amazon EC2, you need to consider the following properties.

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

Create two text files in your home directory. The file `ec2.identity` should contain your access key and the file 
`ec2.credential` should contain your secret key. 

***NOTE:*** *For a full description of the file `stabilizer.properties`, please see the [Stabilizer.Properties File Description section](#stabilizer-properties-file-description).*

### Editing `test.properties`

You need to give the classpath of `Example` test in the file `test.properties` as shown below.


```
class=yourgroupid.ExampleTest
threadCount=1
logFrequency=10000
performanceUpdateFrequency=10000
```

The property `class` defines the actual test case and the rest are the properties you want to bind in your test. If a
property is not defined in this file, the default value of the property given in your test code is used. Please see the `properties` comment in the example code above.

You can also define multiple tests in the file `test.properties` as shown below.

```
foo.class=yourgroupid.ExampleTest
foo.threadCount=1

bar.class=yourgroupid.ExampleTest
bar.threadCount=1

```

This is useful if you want to run multiple tests sequentially, or tests in parallel using the `coordinator --parallel` option. Please see the [Coordinator section](#coordinator) for more information.

### Running the Test

When you are in your working directory, execute the following command to start the test.


```
./run.sh
```

The script `run.sh` is for your convenience which gathers all commands used to perform a test in one script. The following is the content of this example `run.sh` script.

```
???
???
???
???
```

This script performs the following.

 * Start 4 EC2 instances, install Java and the agents.
 * Upload your JARs, run the test using a 2 node test cluster and 2 client machines (the clients generate the load).
 
 
This test runs for 2 minutes. After it is completed, the artifacts (log files) are downloaded in the `workers` directory. Then, it terminates the 4 instances. If you do not want to start/terminate the instances for every run, just comment out the line `provisioner --terminate` in the script `run.sh`. This prevents the machines from being terminated. Please see the [Provisioner section](#provisioner) for more information.

The output of the test looks like the following.

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


??? Post-test information ??? agents.txt ???