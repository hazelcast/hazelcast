

## Example Stabilizer Test


A Stabilizer test is a bit like a JUnit test but there are some fundamental differences. Below you can see an example
test where some counter is being incremented.

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

At the end you see a main method; this is useful if you want to run the test locally to see if works at all.

At the top of the source file you also see see 'properties'. When you create a test, you specify the test property file:

```
class=yourgroupid.ExampleTest
threadCount=1
logFrequency=10000
performanceUpdateFrequency=10000
```

The 'class' property defines the actual test case and the rest are the properties you want to bind in your test. If a
property is not defined in the property file, the default value of the property is used.

You can also define multiple tests in a single property file:

```
foo.class=yourgroupid.ExampleTest
foo.threadCount=1

bar.class=yourgroupid.ExampleTest
bar.threadCount=1

```
This is useful if you want to run multiple tests sequentially, or tests in parallel using the 'coordinator --parallel'
option.
