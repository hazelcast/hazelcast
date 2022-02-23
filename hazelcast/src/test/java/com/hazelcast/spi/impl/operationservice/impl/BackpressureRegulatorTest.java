/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.sequence.CallIdSequence;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithBackpressure;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithoutBackpressure;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_SYNCWINDOW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackpressureRegulatorTest extends HazelcastTestSupport {

    private static final int SYNC_WINDOW = 100;

    private ILogger logger;

    @Before
    public void setup() {
        logger = mock(ILogger.class);
    }

    @Test
    public void testWriteThroughDoesntEnableBackPressure() {
        Config config = new Config();
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator regulator = new BackpressureRegulator(hazelcastProperties, logger);
        CallIdSequence callIdSequence = regulator.newCallIdSequence(ConcurrencyDetection.createEnabled(1000));
        assertEquals(Integer.MAX_VALUE, callIdSequence.getMaxConcurrentInvocations());
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
    public void testConstruction_OneSyncWindowB_syncOnEveryCall() {
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

        CallIdSequence callIdSequence = backpressureRegulator.newCallIdSequence(ConcurrencyDetection.createEnabled(100));

        assertInstanceOf(CallIdSequenceWithBackpressure.class, callIdSequence);
        assertEquals(backpressureRegulator.getMaxConcurrentInvocations(), callIdSequence.getMaxConcurrentInvocations());
    }

    @Test
    public void newCallIdSequence_whenBackPressureDisabled() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        BackpressureRegulator backpressureRegulator = new BackpressureRegulator(hazelcastProperties, logger);

        CallIdSequence callIdSequence = backpressureRegulator.newCallIdSequence(ConcurrencyDetection.createDisabled());

        assertInstanceOf(CallIdSequenceWithoutBackpressure.class, callIdSequence);
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

        int oldSyncDelay = regulator.syncCountDown();

        boolean result = regulator.isSyncForced(op);

        assertFalse(result);
        assertEquals(oldSyncDelay, regulator.syncCountDown());
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

        int oldSyncDelay = regulator.syncCountDown();

        boolean result = regulator.isSyncForced(op);

        assertFalse(result);
        assertEquals(oldSyncDelay, regulator.syncCountDown());
    }

    @Test
    public void isSyncForced_whenPartitionSpecific() {
        BackpressureRegulator regulator = newEnabledBackPressureService();

        BackupAwareOperation op = new PartitionSpecificOperation(10);

        for (int iteration = 0; iteration < 10; iteration++) {
            int initialSyncDelay = regulator.syncCountDown();
            int remainingSyncDelay = initialSyncDelay - 1;
            for (int k = 0; k < initialSyncDelay - 1; k++) {
                boolean result = regulator.isSyncForced(op);
                assertFalse("no sync force expected", result);

                int syncDelay = regulator.syncCountDown();
                assertEquals(remainingSyncDelay, syncDelay);
                remainingSyncDelay--;
            }

            boolean result = regulator.isSyncForced(op);
            assertTrue("sync force expected", result);

            int syncDelay = regulator.syncCountDown();
            assertValidSyncDelay(syncDelay);
        }
    }

    private void assertValidSyncDelay(int synDelay) {
        assertTrue("syncDelayCounter is " + synDelay, synDelay >= (1 - BackpressureRegulator.RANGE) * SYNC_WINDOW);
        assertTrue("syncDelayCounter is " + synDelay, synDelay <= (1 + BackpressureRegulator.RANGE) * SYNC_WINDOW);
    }

    private class UrgentOperation extends Operation implements UrgentSystemOperation, BackupAwareOperation {
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

    private class PartitionSpecificOperation extends Operation implements PartitionAwareOperation, BackupAwareOperation {

        PartitionSpecificOperation(int partitionId) {
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

    private class GenericOperation extends Operation implements BackupAwareOperation {

        GenericOperation() {
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
