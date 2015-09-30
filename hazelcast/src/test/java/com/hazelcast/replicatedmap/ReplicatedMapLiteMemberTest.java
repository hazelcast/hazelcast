package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMessageListener;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.config.ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.EVENT_TOPIC_NAME;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapLiteMemberTest
        extends HazelcastTestSupport {

    @Test
    public void testReplicationEventRegistrations() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance lite = nodeFactory.newHazelcastInstance(new Config().setLiteMember(true));

        final List<Address> expectedRegistrations = asList(getAddress(instance1), getAddress(instance2));
        assertEventRegistrationsEventually(instance1, expectedRegistrations);
        assertEventRegistrationsEventually(instance2, expectedRegistrations);
        // listeners are also registered to lite members but no event will be received from them
        assertEventRegistrationsEventually(lite, expectedRegistrations);
    }

    @Test
    public void testLiteMembersWithDefaultReplicationDelay()
            throws Exception {
        testNoReplicationToLiteMember(buildConfig(DEFAULT_REPLICATION_DELAY_MILLIS, false),
                buildConfig(DEFAULT_REPLICATION_DELAY_MILLIS, true), 5);
    }

    @Test
    public void testLiteMembersWithZeroReplicationDelay()
            throws Exception {
        testNoReplicationToLiteMember(buildConfig(0, false), buildConfig(0, true), 5);
    }

    private void assertEventRegistrationsEventually(final HazelcastInstance instance, final List<Address> expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final InternalEventService eventService = getNodeEngineImpl(instance).getEventService();
                final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);
                assertEquals(expected.size(), registrations.size());
                for (EventRegistration registration : registrations) {
                    assertTrue(expected.contains(registration.getSubscriber()));
                }
            }
        });
    }

    private void testNoReplicationToLiteMember(final Config config, final Config liteConfig, final int assertionDurationInSecs)
            throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance lite = nodeFactory.newHazelcastInstance(liteConfig);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");

        map1.put("key", "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(instance2.getReplicatedMap("default").containsKey("key"));
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final ReplicatedMapService service = getReplicatedMapService(lite);
                assertEquals(0, service.getReplicatedRecordStoresSize());
            }
        }, assertionDurationInSecs);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedMapOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        lite.getReplicatedMap("default");
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedStoreOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        final ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", true);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testGetReplicatedStoreOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        final ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", false);
    }

    @Test
    public void testReplicationListenerNotInvoked() {
        final ReplicatedMessageListener listener = mock(ReplicatedMessageListener.class);
        final HazelcastInstance lite = createSingleLiteMember();
        final ReplicatedMapService service = getReplicatedMapService(lite);
        service.dispatchEvent(new Object(), listener);
        verify(listener, never()).onMessage(null);
    }

    private HazelcastInstance createSingleLiteMember() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        return nodeFactory.newHazelcastInstance(buildConfig(0, true));
    }

    private ReplicatedMapService getReplicatedMapService(HazelcastInstance lite) {
        final NodeEngineImpl nodeEngine = getNodeEngineImpl(lite);
        return nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
    }

    private Config buildConfig(final long replicationDelay, final boolean liteMember) {
        final Config config = new Config();
        final ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("default");
        replicatedMapConfig.setReplicationDelayMillis(replicationDelay);
        config.setLiteMember(liteMember);
        return config;
    }

}
