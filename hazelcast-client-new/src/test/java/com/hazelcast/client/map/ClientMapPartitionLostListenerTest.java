package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.MapPartitionLostListenerStressTest.EventCollectingMapPartitionLostListener;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientMapPartitionLostListenerTest {

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_mapPartitionLostListener_registered() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final String mapName = randomMapName();

        client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));

        assertRegistrationsSizeEventually(instance, mapName, 1);
    }

    @Test
    public void test_mapPartitionLostListener_removed() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final String mapName = randomMapName();

        final String registrationId = client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));
        assertRegistrationsSizeEventually(instance, mapName, 1);

        client.getMap(mapName).removePartitionLostListener(registrationId);
        assertRegistrationsSizeEventually(instance, mapName, 0);
    }

    @Test
    public void test_mapPartitionLostListener_invoked() {
        final String mapName = randomMapName();
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(0);

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance, mapName, 1);

        final MapService mapService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        mapService.onPartitionLost(new InternalPartitionLostEvent(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_mapPartitionLostListener_invoked_fromOtherNode() {
        final String mapName = randomMapName();
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(0);

        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance1, mapName, 1);
        assertRegistrationsSizeEventually(instance2, mapName, 1);

        final MapService mapService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        mapService.onPartitionLost(new InternalPartitionLostEvent(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    private void assertMapPartitionLostEventEventually(final EventCollectingMapPartitionLostListener listener,
                                                       final int partitionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final List<MapPartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                assertEquals(partitionId, events.get(0).getPartitionId());

            }
        });
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final String mapName, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
                assertEquals(size, registrations.size());

            }
        });
    }

}
