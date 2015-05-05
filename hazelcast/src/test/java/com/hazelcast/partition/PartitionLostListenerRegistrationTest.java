package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.partition.InternalPartitionService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PartitionLostListenerRegistrationTest
        extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void test_addPartitionLostListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addPartitionLostListener(null);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredProgramatically() {
        final HazelcastInstance instance = createHazelcastInstance();

        final String id = instance.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertNotNull(id);

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void test_partitionLostListener_whenListenerRegisteredViaConfiguration() {
        final Config config = new Config();
        config.addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));

        final HazelcastInstance instance = createHazelcastInstance(config);
        assertRegistrationsSizeEventually(instance, 1);
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int expectedSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                        assertEquals(expectedSize, eventService.getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC).size());
                    }
                });
            }
        });
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredTwice() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final PartitionLostListener listener = mock(PartitionLostListener.class);

        final String id1 = partitionService.addPartitionLostListener(listener);
        final String id2 = partitionService.addPartitionLostListener(listener);

        assertNotEquals(id1, id2);
        assertRegistrationsSizeEventually(instance, 2);
    }

    @Test
    public void test_removeMigrationListener_whenRegisteredListenerRemovedSuccessfully() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final PartitionLostListener listener = mock(PartitionLostListener.class);

        final String id1 = partitionService.addPartitionLostListener(listener);
        final boolean result = partitionService.removePartitionLostListener(id1);

        assertTrue(result);
        assertRegistrationsSizeEventually(instance, 0);
    }


    @Test
    public void test_removeMigrationListener_whenNonExistingRegistrationIdRemoved() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final boolean result = partitionService.removePartitionLostListener("notexist");
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void test_removeMigrationListener_whenNullRegistrationIdRemoved() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        partitionService.removePartitionLostListener(null);
    }

}
