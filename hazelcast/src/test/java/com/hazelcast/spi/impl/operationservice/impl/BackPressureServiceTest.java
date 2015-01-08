package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BackPressureServiceTest {

    public final static int SYNC_WINDOW = 100;

    private ILogger logger;

    @Before
    public void setup() {
        logger = mock(ILogger.class);
    }

    @Test
    public void testBackPressureDisabledByDefault() {
        Config config = new Config();
        GroupProperties groupProperties = new GroupProperties(config);
        BackPressureService service = new BackPressureService(groupProperties, logger);

        assertFalse(service.isBackPressureEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstruction_invalidSyncWindow() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true");
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "" + 0);
        GroupProperties groupProperties = new GroupProperties(config);
        new BackPressureService(groupProperties, logger);
    }

    @Test
    public void whenUrgentOperation_thenFalse() {
        BackPressureService service = newEnabledBackPressureService();
        Operation operation = new UrgentOperation();

        boolean result = service.requiresBackPressure(operation);

        assertFalse(result);
    }

    @Test
    public void whenDisabled_thenFalse() {
        BackPressureService service = newDisabledBackPressureService();
        PartitionSpecificOperation op = new PartitionSpecificOperation();

        boolean result = service.requiresBackPressure(op);

        assertFalse(result);

        // verify that no sync-delays have been created
        AtomicInteger syncDelay = service.getSyncDelay(op.getPartitionId());
        assertNull(syncDelay);
    }

    @Test
    public void partitionSpecific() {
        whenNormalOperation(true);
    }

    public void whenNormalOperation(boolean partitionSpecific) {
        BackPressureService service = newEnabledBackPressureService();

        Operation op;
        if (partitionSpecific) {
            PartitionSpecificOperation partitionOp = new PartitionSpecificOperation();
            partitionOp.setPartitionId(0);
            op = partitionOp;
        } else {
            op = new GenericOperation();
            op.setPartitionId(Operation.GENERIC_PARTITION_ID);
        }

        int backPressureCount = 0;
        int rounds = 10;
        for (int k = 0; k < SYNC_WINDOW * rounds; k++) {
            boolean result = service.requiresBackPressure(op);
            if (result) {
                backPressureCount++;
            }
        }

        assertTrue(backPressureCount >= rounds - 1);
        assertTrue(backPressureCount <= rounds + 1);
    }

    private class UrgentOperation extends AbstractOperation implements UrgentSystemOperation {
        @Override
        public void run() throws Exception {
        }
    }

    private class PartitionSpecificOperation extends AbstractOperation implements PartitionAwareOperation {

        @Override
        public void run() throws Exception {
        }
    }

    private class GenericOperation extends AbstractOperation {

        @Override
        public void run() throws Exception {
        }
    }

    private BackPressureService newEnabledBackPressureService() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true");
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "" + SYNC_WINDOW);
        GroupProperties groupProperties = new GroupProperties(config);
        return new BackPressureService(groupProperties, logger);
    }

    private BackPressureService newDisabledBackPressureService() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "false");
        GroupProperties groupProperties = new GroupProperties(config);
        return new BackPressureService(groupProperties, logger);
    }
}
