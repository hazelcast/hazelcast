package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class BackPressureStressTest extends HazelcastTestSupport {

    // to stress the back-pressure (to expose the problem quickly) we are going to attach some additional data to each
    // operation. If many operations are being stored (so no back pressure) then you will very quickly run into OOME.
    // So by increasing this, you will increase the chance to see problems.
    // On my machine with MEMORY_STRESS_PAYLOAD_SIZE=100000 it takes a very short time (under a minute) to run out of memory if
    // back pressure is disabled. If you run these tests using a profiler, make sure you keep an eye out on Memory usage and GC
    // activity. It is very easy to detect when back pressure is disabled.
    public static final int MEMORY_STRESS_PAYLOAD_SIZE = 100000;

    private static final int runningTimeMs = (int) TimeUnit.SECONDS.toMillis(300);

    private final Random random = new Random();
    private final AtomicLong completedCall = new AtomicLong();
    private final AtomicLong failedOperationCount = new AtomicLong();
    private final AtomicLong globalOperationCount = new AtomicLong();
    private final AtomicBoolean stop = new AtomicBoolean();

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private OperationServiceImpl localOperationService;

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

    // works
    @Test
    public void test_asyncInvocation_and_hasResponse() throws Exception {
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = true;
                stressThread.syncInvocation = false;
                stressThread.runDelayMs = 1;
                stressThread.shouldBackup = false;
                stressThread.asyncBackups = 0;
                stressThread.syncBackups = 0;
                stressThread.backupRunDelayMs = 0;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    // tbd
    // in this case we rely on the {@link ForcedSyncResponse}.
    @Test
    public void test_asyncInvocation_and_hasNoResponse() throws Exception {
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = false;
                stressThread.syncInvocation = false;
                stressThread.runDelayMs = 1;
                stressThread.shouldBackup = false;
                stressThread.asyncBackups = 0;
                stressThread.syncBackups = 0;
                stressThread.backupRunDelayMs = 0;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    // works (although don't know why)
    @Test
    public void test_asyncInvocation_and_syncBackups() throws Exception{
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = true;
                stressThread.syncInvocation = false;
                stressThread.runDelayMs = 0;
                stressThread.shouldBackup = false;
                stressThread.asyncBackups = 0;
                stressThread.syncBackups = 1;
                stressThread.backupRunDelayMs = 0;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    @Test
    public void test_asyncInvocation_and_asyncBackups()throws Exception {
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = true;
                stressThread.syncInvocation = false;
                stressThread.runDelayMs = 0;
                stressThread.shouldBackup = true;
                stressThread.asyncBackups = 1;
                stressThread.syncBackups = 0;
                stressThread.backupRunDelayMs = 1;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    // works
    @Test
    public void test_syncInvocation_and_asyncBackups() throws Exception {
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = true;
                stressThread.syncInvocation = true;
                stressThread.runDelayMs = 0;
                stressThread.shouldBackup = true;
                stressThread.asyncBackups = 1;
                stressThread.syncBackups = 0;
                stressThread.backupRunDelayMs = 1;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    // works
    @Test
    public void test_syncInvocation_and_syncBackups_and_asyncBackups() throws Exception {
        test(new StressThreadFactory() {
            @Override
            public StressThread create() {
                StressThread stressThread = new StressThread();
                stressThread.returnsResponse = true;
                stressThread.syncInvocation = true;
                stressThread.runDelayMs = 0;
                stressThread.shouldBackup = true;
                stressThread.asyncBackups = 1;
                stressThread.syncBackups = 1;
                stressThread.backupRunDelayMs = 1;
                stressThread.partitionId = getPartitionId(remote);
                return stressThread;
            }
        });
    }

    public void test(StressThreadFactory stressThreadFactory) throws Exception {
        StressThread stressThread = stressThreadFactory.create();

        stressThread.start();

        sleepMillis(runningTimeMs);

        stop.set(true);

        stressThread.join();

        System.out.println("Completed with asynchronous calls, waiting for everything to complete");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("the number of completed calls doesn't match the number of expected calls",
                        globalOperationCount.get(), completedCall.get());
            }
        });

        assertEquals(0, failedOperationCount.get());

        long count = localOperationService.backPressureService.backPressureCount();
        System.out.println("Backpressure count: " + count);
    }

    private final static AtomicLong THREAD_ID_GENERATOR = new AtomicLong();

    private class StressThread extends Thread {

        public int partitionId;
        public boolean syncInvocation;

        public int asyncBackups;
        public int syncBackups;
        public boolean shouldBackup;
        public boolean returnsResponse;
        public int runDelayMs = 1;
        public int backupRunDelayMs = 0;

        public StressThread() {
            super("StressThread-" + THREAD_ID_GENERATOR.incrementAndGet());
        }

        @Override
        public void run() {
            long operationCount = 0;

            long lastSecond = System.currentTimeMillis() / 1000;

            while (!stop.get()) {
                long currentSecond = System.currentTimeMillis() / 1000;
                if (currentSecond != lastSecond) {
                    lastSecond = currentSecond;
                    System.out.println(" at: " + operationCount);
                }

                long expectedResult = random.nextLong();
                DummyOperation operation = new DummyOperation(expectedResult);

                operation.returnsResponse = returnsResponse;
                operation.syncBackups = syncBackups;
                operation.asyncBackups = asyncBackups;
                operation.runDelayMs = runDelayMs;
                operation.backupRunDelayMs = backupRunDelayMs;
                operation.shouldBackup = shouldBackup;

                if (syncInvocation) {
                    syncInvoke(operation);
                } else {
                    asyncInvoke(operation);
                }
                operationCount++;
                globalOperationCount.incrementAndGet();
            }
        }

        private void asyncInvoke(DummyOperation operation) {
            final long expectedResult = operation.result;

            InternalCompletableFuture f = localOperationService.invokeOnPartition(null, operation, partitionId);
            f.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    completedCall.incrementAndGet();

                    if (!new Long(expectedResult).equals(response)) {
                        System.out.println("Wrong result received, expecting: " + expectedResult + " but found:" + response);
                        failedOperationCount.incrementAndGet();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    completedCall.incrementAndGet();
                    failedOperationCount.incrementAndGet();
                    t.printStackTrace();
                }
            });
        }

        private void syncInvoke(DummyOperation operation) {
            final Long expectedResult = operation.result;

            InternalCompletableFuture f = localOperationService.invokeOnPartition(null, operation, partitionId);
            completedCall.incrementAndGet();

            try {
                Long result = (Long) f.getSafely();

                if (!expectedResult.equals(result)) {
                    failedOperationCount.incrementAndGet();
                }
            } catch (Exception e) {
                failedOperationCount.incrementAndGet();
                e.printStackTrace();
            }
        }
    }

    private interface StressThreadFactory {
        StressThread create();
    }

    static class DummyOperation extends AbstractOperation implements BackupAwareOperation {
        long result;
        int asyncBackups;
        int syncBackups;
        boolean shouldBackup = false;
        boolean returnsResponse = true;
        int runDelayMs = 1;
        int backupRunDelayMs = 0;

        public DummyOperation() {
        }

        public DummyOperation(long result) {
            this.result = result;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(runDelayMs);
        }

        @Override
        public boolean returnsResponse() {
            return returnsResponse;
        }

        @Override
        public boolean shouldBackup() {
            return shouldBackup;
        }

        @Override
        public int getSyncBackupCount() {
            return syncBackups;
        }

        @Override
        public int getAsyncBackupCount() {
            return asyncBackups;
        }

        @Override
        public Operation getBackupOperation() {
            DummyBackupOperation backupOperation = new DummyBackupOperation();
            backupOperation.runDelayMs = backupRunDelayMs;
            return backupOperation;
        }

        @Override
        public Object getResponse() {
            return result;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(result);
            out.writeBoolean(returnsResponse);
            out.writeInt(runDelayMs);

            out.writeBoolean(shouldBackup);
            out.writeInt(syncBackups);
            out.writeInt(asyncBackups);
            out.writeInt(backupRunDelayMs);
            byte[] bytes = new byte[MEMORY_STRESS_PAYLOAD_SIZE];
            out.writeByteArray(bytes);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            result = in.readLong();
            returnsResponse = in.readBoolean();
            runDelayMs = in.readInt();

            shouldBackup = in.readBoolean();
            syncBackups = in.readInt();
            asyncBackups = in.readInt();
            backupRunDelayMs = in.readInt();

            // reading the stress payload
            in.readByteArray();
        }
    }

    public static class DummyBackupOperation extends AbstractOperation implements BackupOperation {
        private int runDelayMs;

        @Override
        public void run() throws Exception {
            Thread.sleep(runDelayMs);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(runDelayMs);

            byte[] bytes = new byte[MEMORY_STRESS_PAYLOAD_SIZE];
            out.writeByteArray(bytes);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            runDelayMs = in.readInt();

            // reading the stress payload
            in.readByteArray();
        }
    }
}
