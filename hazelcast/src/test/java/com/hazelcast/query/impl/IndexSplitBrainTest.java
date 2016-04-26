package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.PartitionAwareKey;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IndexSplitBrainTest extends HazelcastTestSupport {

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIndexDoesNotReturnStaleResultsAfterSplit() {
        String mapName = randomMapName();
        Config config = newConfig(LatestUpdateMapMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMembershipListener membershipListener = new TestMembershipListener(1);
        h2.getCluster().addMembershipListener(membershipListener);
        TestLifecycleListener lifecycleListener = new TestLifecycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifecycleListener);

        RealtimeCall call = new RealtimeCall();
        String key = generateKeyOwnedBy(h1);
        call.setId(key);
        call.setClusterUUID(key);

        IMap<PartitionAwareKey<String, String>, RealtimeCall> map1 = h1.getMap(mapName);
        IMap<PartitionAwareKey<String, String>, RealtimeCall> map2 = h2.getMap(mapName);

        map1.put(call.getAffinityKey(), call);

        sleepMillis(1);
        assertNotNull("entry should be in map2 before split", map2.get(call.getAffinityKey()));

        closeConnectionBetween(h1, h2);

        assertOpenEventually(membershipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        map1 = h1.getMap(mapName);
        map1.remove(call.getAffinityKey());

        sleepMillis(1);
        map2 = h2.getMap(mapName);
        assertNotNull("entry should be in map2 in split", map2.get(call.getAffinityKey()));

        assertOpenEventually(lifecycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        map1 = h1.getMap(mapName);
        assertNotNull("entry should be in map1", map1.get(call.getAffinityKey()));

        map1.remove(call.getAffinityKey());
        assertNull("map1 should be null", map1.get(call.getAffinityKey()));
        assertNull("map2 should be null", map2.get(call.getAffinityKey()));

        for (int i = 0; i < 100; i++) {
            Collection<RealtimeCall> calls = map1.values(Predicates.equal("id", call.getId()));
            System.out.println("Map 1 query by uuid: " + calls.size());
            assert calls.size() == 0;
            calls = map2.values(Predicates.equal("id", call.getId()));
            System.out.println("Map 2 query by uuid: " + calls.size());
            assert calls.size() == 0;
            sleepMillis(5);
        }
    }

    private Config newConfig(String mergePolicy, String mapName) {
        Config config = new Config();
        setCommonProperties(config);
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMergePolicy(mergePolicy);
        mapConfig.setBackupCount(1);
        mapConfig.setReadBackupData(true);
        mapConfig.setStatisticsEnabled(true);
        mapConfig.setMaxIdleSeconds(0);
        mapConfig.setTimeToLiveSeconds(0);
        mapConfig.addMapIndexConfig(new MapIndexConfig("id", false));
        config.setNetworkConfig(this.getLocalhostTcpIpNetworkConfig(6701));
        config.getGroupConfig().setName(mapName);
        config.getGroupConfig().setPassword(mapName);
        return config;
    }

    private class TestLifecycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifecycleListener(int countdown) {
            latch = new CountDownLatch(countdown);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }
    }

    private class TestMembershipListener implements MembershipListener {

        final CountDownLatch latch;

        TestMembershipListener(int countdown) {
            latch = new CountDownLatch(countdown);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {

        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            latch.countDown();
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }
    }

    protected NetworkConfig getLocalhostTcpIpNetworkConfig(int port) {
        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort(port);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1");
        InterfacesConfig interfacesConfig = networkConfig.getInterfaces();
        interfacesConfig.setEnabled(true);
        interfacesConfig.setInterfaces(Collections.singleton("127.0.0.*"));
        return networkConfig;
    }

    protected void setCommonProperties(Config config) {
        config.setProperty(GroupProperty.LOGGING_TYPE.getName(), "log4j");
        config.setProperty(GroupProperty.PHONE_HOME_ENABLED.getName(), "false");
        config.setProperty("hazelcast.mancenter.enabled", "false");
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
        config.setProperty(GroupProperty.CONNECT_ALL_WAIT_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "2");
        config.setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        config.setProperty(GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "10");
        config.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "10");
        config.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        config.setProperty("java.net.preferIPv4Stack", "true");
        config.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        config.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    public static class RealtimeCall implements DataSerializable {
        private String id;
        private String clusterUUID;

        public RealtimeCall() {
        }

        public String getId() {
            return this.id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getClusterUUID() {
            return this.clusterUUID;
        }

        public void setClusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
        }

        public PartitionAwareKey<String, String> getAffinityKey() {
            return new PartitionAwareKey<String, String>(getId(), getClusterUUID());
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(this.id);
            out.writeUTF(this.clusterUUID);
        }

        public void readData(ObjectDataInput in) throws IOException {
            this.id = in.readUTF();
            this.clusterUUID = in.readUTF();
        }
    }
}
