package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.MapPartitionLostListenerStressTest.EventCollectingMapPartitionLostListener;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.client.impl.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapPartitionLostListenerTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_mapPartitionLostListener_registered() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final String mapName = randomMapName();

        client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));

        assertRegistrationEventually(instance, mapName, true);
    }

    @Test
    public void test_mapPartitionLostListener_removed() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final String mapName = randomMapName();

        final String registrationId = client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));
        assertRegistrationEventually(instance, mapName, true);

        assertTrue(client.getMap(mapName).removePartitionLostListener(registrationId));
        assertRegistrationEventually(instance, mapName, false);
    }

    @Test
    public void test_mapPartitionLostListener_invoked() {
        final String mapName = randomMapName();
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(0);

        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        warmUpPartitions(instance, client);

        final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        final MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        final int partitionId = 5;
        mapService.onPartitionLost(new IPartitionLostEvent(partitionId, 0, null));

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

    @Test
    public void test_mapPartitionLostListener_invoked_fromOtherNode() {
        final String mapName = randomMapName();
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(0);

        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        assertMapContainerCreated(instance1, mapName);
        assertMapContainerCreated(instance2, mapName);

        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final Address clientOwnerAddress = clientInstanceImpl.getClientClusterService().getOwnerConnectionAddress();

        final HazelcastInstance other = getAddress(instance1).equals(clientOwnerAddress) ? instance2 : instance1;

        final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        assertRegistrationEventually(instance1, mapName, true);
        assertRegistrationEventually(instance2, mapName, true);

        final MapService mapService = getNode(other).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        mapService.onPartitionLost(new IPartitionLostEvent(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    private void assertMapContainerCreated(final HazelcastInstance instance, final String mapName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
                assertNotNull(mapService.getMapServiceContext().getMapContainer(mapName));
            }
        });
    }

    private void assertRegistrationEventually(final HazelcastInstance instance, final String mapName, final boolean shouldBeRegistered) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();

                boolean registered = false;
                for (EventRegistration registration : eventService.getRegistrations(SERVICE_NAME, mapName)) {
                    if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                        registered = true;
                        break;
                    }
                }

                if (shouldBeRegistered != registered) {
                    fail("shouldBeRegistered: " + shouldBeRegistered + " registered: " + registered);
                }
            }
        });
    }

}
