package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicBackPressureServiceTest {

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
        BasicBackPressureService service = new BasicBackPressureService(groupProperties, logger);
        assertFalse(service.isBackPressureEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstruction_invalidSyncWindow() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true");
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "" + 0);
        GroupProperties groupProperties = new GroupProperties(config);
        new BasicBackPressureService(groupProperties, logger);
    }

    // ========================== clean =================

    @Test
    public void cleanup_whenDisabled() {
        BasicBackPressureService service = newDisabledBackPressureService();
        service.cleanup();
    }

    @Test
    public void cleanup_whenConnectionIsAlive() {
        BasicBackPressureService service = newEnabledBackPressureService();
        GenericOperation op = new GenericOperation();

        Connection connection = mock(Connection.class);
        when(connection.isAlive()).thenReturn(true);
        OperationAccessor.setConnection(op, connection);

        service.isBackPressureNeeded(op);

        service.cleanup();

        assertNotNull(service.getSyncDelays(connection));
    }

    @Test
    public void cleanup_whenConnectionIsNotAlive() {
        BasicBackPressureService service = newEnabledBackPressureService();
        GenericOperation op = new GenericOperation();

        Connection connection = mock(Connection.class);
        when(connection.isAlive()).thenReturn(false);
        OperationAccessor.setConnection(op, connection);

        service.isBackPressureNeeded(op);

        service.cleanup();

        assertNull(service.getSyncDelays(connection));
    }

    // ========================== isBackPressureNeeded =================

    @Test
    public void whenUrgentOperation_thenFalse() {
        BasicBackPressureService service = newEnabledBackPressureService();
        Operation operation = new UrgentOperation();

        boolean result = service.isBackPressureNeeded(operation);

        assertFalse(result);
    }

    @Test
    public void whenDisabled_thenFalse() {
        BasicBackPressureService service = newDisabledBackPressureService();
        PartitionSpecificOperation op = new PartitionSpecificOperation();

        boolean result = service.isBackPressureNeeded(op);

        assertFalse(result);

        // verify that no sync-delays have been created
        AtomicInteger syncDelay = service.getSyncDelay(null, op.getPartitionId());
        assertNull(syncDelay);
    }

    @Test
    public void partitionSpecific_localCall_() {
        whenNormalOperation(true, false);
    }

    @Test
    public void partitionSpecific_remoteCall() {
        whenNormalOperation(true, true);
    }

    public void whenNormalOperation(boolean partitionSpecific, boolean remoteCall) {
        BasicBackPressureService service = newEnabledBackPressureService();

        Operation op;
        if (partitionSpecific) {
            PartitionSpecificOperation partitionOp = new PartitionSpecificOperation();
            partitionOp.setPartitionId(0);
            op = partitionOp;
        } else {
            op = new GenericOperation();
            op.setPartitionId(-1);
        }

        Connection connection = null;
        if (remoteCall) {
            connection = mock(Connection.class);
            OperationAccessor.setConnection(op, connection);
        }

        int expected = SYNC_WINDOW - 1;
        for (int k = 0; k < SYNC_WINDOW; k++) {
            boolean result = service.isBackPressureNeeded(op);
            assertFalse("no back pressure expected", result);

            AtomicInteger syncDelay = service.getSyncDelay(connection, op.getPartitionId());
            assertEquals(expected, syncDelay.get());
            expected--;
        }

        boolean result = service.isBackPressureNeeded(op);
        assertTrue("back pressure expected", result);

        AtomicInteger syncDelay = service.getSyncDelay(connection, op.getPartitionId());
        assertValidSyncDelay(syncDelay);
    }

    private void assertValidSyncDelay(AtomicInteger synDelay) {
        assertTrue("syncDelay is " + synDelay, synDelay.get() >= (1 - BasicBackPressureService.RANGE) * SYNC_WINDOW);
        assertTrue("syncDelay is " + synDelay, synDelay.get() <= (1 + BasicBackPressureService.RANGE) * SYNC_WINDOW);
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

    private BasicBackPressureService newEnabledBackPressureService() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true");
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "" + SYNC_WINDOW);
        GroupProperties groupProperties = new GroupProperties(config);
        return new BasicBackPressureService(groupProperties, logger);
    }

    private BasicBackPressureService newDisabledBackPressureService() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "false");
        GroupProperties groupProperties = new GroupProperties(config);
        return new BasicBackPressureService(groupProperties, logger);
    }
}
