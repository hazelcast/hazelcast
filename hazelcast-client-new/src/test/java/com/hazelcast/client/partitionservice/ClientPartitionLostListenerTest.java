package com.hazelcast.client.partitionservice;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientPartitionLostListenerTest {

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_partitionLostListener_registered() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void test_partitionLostListener_removed() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final String registrationId = client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertRegistrationsSizeEventually(instance, 1);

        client.getPartitionService().removePartitionLostListener(registrationId);
        assertRegistrationsSizeEventually(instance, 0);
    }

    @Test
    public void test_partitionLostListener_invoked() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();

        client.getPartitionService().addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance, 1);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        partitionService.onPartitionLost(new InternalPartitionLostEvent(partitionId, 0, null));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked_fromOtherNode() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        client.getPartitionService().addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance1, 1);
        assertRegistrationsSizeEventually(instance2, 1);

        final InternalPartitionServiceImpl partitionService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        partitionService.onPartitionLost(new InternalPartitionLostEvent(partitionId, 0, null));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                final Collection<EventRegistration> registrations = eventService
                        .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
                assertEquals(size, registrations.size());

            }
        });
    }

    private void assertPartitionLostEventEventually(final EventCollectingPartitionLostListener listener, final int partitionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final List<PartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                assertEquals(partitionId, events.get(0).getPartitionId());

            }
        });
    }
}
