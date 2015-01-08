package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class BackPressureTest extends HazelcastTestSupport {
    private final static AtomicLong ID_GENERATOR = new AtomicLong();

    private int runningTimeMs = (int) TimeUnit.SECONDS.toMillis(30);
    private Random random = new Random();
    private HazelcastInstance local;
    private HazelcastInstance remote;
    private OperationServiceImpl localOperationService;
    private final AtomicLong completedCall = new AtomicLong();
    private final AtomicLong failedCalls = new AtomicLong();
    private final AtomicLong globalOperationCount = new AtomicLong();
    private final AtomicBoolean stop = new AtomicBoolean();

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true")
                .setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "100");
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);
        local = cluster[0];
        remote = cluster[1];

        localOperationService = (OperationServiceImpl) getOperationService(local);
    }

    // todo: we need to test async calls without backups
    // async calls with sync backups
    // async calls with async backups
    // sync calls with async backups

    @Test
    public void test_whenPoundingSinglePartition() throws InterruptedException {
        PoundThread poundThread = new PoundThread(getPartitionId(remote));
        poundThread.start();

        sleepMillis(runningTimeMs);

        stop.set(true);

        poundThread.join();

        System.out.println("Completed with asynchronous calls, waiting for everything to complete");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("the number of completed calls doesn't match the number of expected calls",
                        globalOperationCount.get(), completedCall.get());
            }
        });

        assertEquals(0, failedCalls.get());

        long count = localOperationService.backPressureService.backPressureCount();
        System.out.println("Backpressure count: " + count);
    }

    private class PoundThread extends Thread {

        private final int partitionId;

        public PoundThread(int partitionId) {
            super("StressThread-" + ID_GENERATOR.incrementAndGet());
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            long operationCount = 0;

            while (!stop.get()) {
                if (operationCount % 100000 == 0) {
                    System.out.println(" at: " + operationCount);
                }

                final long result = random.nextLong();
                DummyOperation operation = new DummyOperation(result);

                InternalCompletableFuture f = localOperationService.invokeOnPartition(null, operation, partitionId);
                f.andThen(new ExecutionCallback() {
                    @Override
                    public void onResponse(Object response) {
                        completedCall.incrementAndGet();

                        if (!new Long(result).equals(response)) {
                            System.out.println("Wrong result received, expecting: " + result + " but found:" + response);
                            failedCalls.incrementAndGet();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completedCall.incrementAndGet();
                        failedCalls.incrementAndGet();
                        t.printStackTrace();
                    }
                });

                operationCount++;
                globalOperationCount.incrementAndGet();
            }
        }
    }

    private static class DummyOperation extends AbstractOperation implements PartitionAwareOperation {

        private long result;

        public DummyOperation() {
        }

        public DummyOperation(long result) {
            this.result = result;
        }

        @Override
        public void run() throws Exception {
            //Thread.sleep(100);
        }

        @Override
        public Object getResponse() {
            return result;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(result);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            result = in.readLong();
        }
    }
}
