package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_SYNCWINDOW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackpressureRegulatorTest extends HazelcastTestSupport {

    private final static int SYNC_WINDOW = 100;

    private ILogger logger;

    @Before
    public void setup() {
        logger = mock(ILogger.class);
    }

    @Test
    public void testBackPressureDisabledByDefault() {
        Config config = new Config();
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator regulator = new BackpressureRegulator(hazelcastProperties, logger);
        assertFalse(regulator.isEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstruction_invalidSyncWindow() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "true");
        config.setProperty(BACKPRESSURE_SYNCWINDOW.getName(), "0");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);

        new BackpressureRegulator(hazelcastProperties, logger);
    }

    @Test
    public void testConstruction_OneSyncWindow_syncOnEveryCall() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "true");
        config.setProperty(BACKPRESSURE_SYNCWINDOW.getName(), "1");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator regulator = new BackpressureRegulator(hazelcastProperties, logger);
        for (int k = 0; k < 1000; k++) {
            PartitionSpecificOperation op = new PartitionSpecificOperation(10);
            assertTrue(regulator.isSyncForced(op));
        }
    }

    // ========================== newCallIdSequence =================

    @Test
    public void newCallIdSequence_whenBackPressureEnabled() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "true");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator backpressureRegulator = new BackpressureRegulator(hazelcastProperties, logger);

        CallIdSequence callIdSequence = backpressureRegulator.newCallIdSequence();

        assertInstanceOf(CallIdSequence.CallIdSequenceWithBackpressure.class, callIdSequence);
        assertEquals(backpressureRegulator.getMaxConcurrentInvocations(), callIdSequence.getMaxConcurrentInvocations());
    }

    @Test
    public void newCallIdSequence_whenBackPressureDisabled() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator backpressureRegulator = new BackpressureRegulator(hazelcastProperties, logger);

        CallIdSequence callIdSequence = backpressureRegulator.newCallIdSequence();

        assertInstanceOf(CallIdSequence.CallIdSequenceWithoutBackpressure.class, callIdSequence);
    }

    // ========================== isSyncForced =================

    @Test
    public void isSyncForced_whenUrgentOperation_thenFalse() {
        BackpressureRegulator regulator = newEnabledBackPressureService();
        UrgentOperation operation = new UrgentOperation();
        operation.setPartitionId(1);

        boolean result = regulator.isSyncForced(operation);

        assertFalse(result);
    }

    @Test
    public void isSyncForced_whenDisabled_thenFalse() {
        BackpressureRegulator regulator = newDisabledBackPressureService();
        PartitionSpecificOperation op = new PartitionSpecificOperation(10);

        int oldSyncDelay = regulator.syncDelay(op);

        boolean result = regulator.isSyncForced(op);

        assertFalse(result);
        assertEquals(oldSyncDelay, regulator.syncDelay(op));
    }

    @Test
    public void isSyncForced_whenNoAsyncBackups_thenFalse() {
        BackpressureRegulator regulator = newEnabledBackPressureService();
        PartitionSpecificOperation op = new PartitionSpecificOperation(10) {
            @Override
            public int getAsyncBackupCount() {
                return 0;
            }
        };

        int oldSyncDelay = regulator.syncDelay(op);

        boolean result = regulator.isSyncForced(op);

        assertFalse(result);
        assertEquals(oldSyncDelay, regulator.syncDelay(op));
    }

    @Test
    public void isSyncForced_whenPartitionSpecific() {
        BackpressureRegulator regulator = newEnabledBackPressureService();

        BackupAwareOperation op = new PartitionSpecificOperation(10);

        for (int iteration = 0; iteration < 10; iteration++) {
            int initialSyncDelay = regulator.syncDelay((Operation) op);
            int remainingSyncDelay = initialSyncDelay - 1;
            for (int k = 0; k < initialSyncDelay - 1; k++) {
                boolean result = regulator.isSyncForced(op);
                assertFalse("no sync force expected", result);

                int syncDelay = regulator.syncDelay((Operation) op);
                assertEquals(remainingSyncDelay, syncDelay);
                remainingSyncDelay--;
            }

            boolean result = regulator.isSyncForced(op);
            assertTrue("sync force expected", result);

            int syncDelay = regulator.syncDelay((Operation) op);
            assertValidSyncDelay(syncDelay);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void isSyncForced_whenGeneric_thenIllegalArgumentException() {
        GenericOperation op = new GenericOperation();
        BackpressureRegulator regulator = newEnabledBackPressureService();
        regulator.isSyncForced(op);
    }

    private void assertValidSyncDelay(int synDelay) {
        assertTrue("syncDelayCounter is " + synDelay, synDelay >= (1 - BackpressureRegulator.RANGE) * SYNC_WINDOW);
        assertTrue("syncDelayCounter is " + synDelay, synDelay <= (1 + BackpressureRegulator.RANGE) * SYNC_WINDOW);
    }

    private class UrgentOperation extends AbstractOperation implements UrgentSystemOperation, BackupAwareOperation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return false;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 1;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }

    private class PartitionSpecificOperation extends AbstractOperation implements PartitionAwareOperation, BackupAwareOperation {

        public PartitionSpecificOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 1;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }

    private class GenericOperation extends AbstractOperation implements BackupAwareOperation {

        public GenericOperation() {
            setPartitionId(-1);
        }


        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 1;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }

    private BackpressureRegulator newEnabledBackPressureService() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "true");
        config.setProperty(BACKPRESSURE_SYNCWINDOW.getName(), String.valueOf(SYNC_WINDOW));
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        return new BackpressureRegulator(hazelcastProperties, logger);
    }

    private BackpressureRegulator newDisabledBackPressureService() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        return new BackpressureRegulator(hazelcastProperties, logger);
    }
}
