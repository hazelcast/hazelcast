package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MergePolicyTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testLatestUpdateMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(LatestUpdateMapMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        map1.put("key1", "value");
        //prevent updating at the same time
        sleepMillis(1);
        map2.put("key1", "LatestUpdatedValue");
        map2.put("key2", "value2");
        //prevent updating at the same time
        sleepMillis(1);
        map1.put("key2", "LatestUpdatedValue2");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        IMap<Object, Object> mapTest = h1.getMap(mapName);
        assertEquals("LatestUpdatedValue", mapTest.get("key1"));
        assertEquals("LatestUpdatedValue2", mapTest.get("key2"));
    }

    @Test
    public void testHigherHitsMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(HigherHitsMapMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        map1.put("key1", "higherHitsValue");
        map1.put("key2", "value2");
        //increase hits number
        map1.get("key1");
        map1.get("key1");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put("key1", "value1");
        map2.put("key2", "higherHitsValue2");
        //increase hits number
        map2.get("key2");
        map2.get("key2");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("higherHitsValue", mapTest.get("key1"));
        assertEquals("higherHitsValue2", mapTest.get("key2"));
    }

    @Test
    public void testPutIfAbsentMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(PutIfAbsentMapMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        map1.put("key1", "PutIfAbsentValue1");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put("key1", "value");
        map2.put("key2", "PutIfAbsentValue2");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("PutIfAbsentValue1", mapTest.get("key1"));
        assertEquals("PutIfAbsentValue2", mapTest.get("key2"));
    }

    @Test
    public void testPassThroughMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(PassThroughMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, "value");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put(key, "passThroughValue");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("passThroughValue", mapTest.get(key));
    }

    @Test
    public void testCustomMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(CustomMergePolicy.class.getName(), mapName);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, "value");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put(key, Integer.valueOf(1));

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertNotNull(mapTest.get(key));
        assertTrue(mapTest.get(key) instanceof Integer);
    }

    private Config newConfig(String mergePolicy, String mapName) {
        Config config = getConfig();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config.getGroupConfig().setName(generateRandomString(10));
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMergePolicy(mergePolicy);
        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifeCycleListener(int countdown) {
            latch = new CountDownLatch(countdown);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }
    }

    private class TestMemberShipListener implements MembershipListener {

        final CountDownLatch latch;

        TestMemberShipListener(int countdown) {
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

}
