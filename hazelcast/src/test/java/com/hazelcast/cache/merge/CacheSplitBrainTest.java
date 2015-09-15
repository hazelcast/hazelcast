package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.OriginalTypeAwareCacheMergePolicy;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.merge.policy.HigherHitsCacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.LatestAccessCacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.PassThroughCacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.PutIfAbsentCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class CacheSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testLatestAccessCacheMergePolicy() {
        String cacheName = randomMapName();
        Config config = newConfig();
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

        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(h1);
        CachingProvider cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(h2);

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig cacheConfig = newCacheConfig(cacheName, LatestAccessCacheMergePolicy.class.getName());

        Cache cache1 = cacheManager1.createCache(cacheName, cacheConfig);
        Cache cache2 = cacheManager2.createCache(cacheName, cacheConfig);

        cache1.put("key1", "value");
        cache1.get("key1"); // Access to record

        // Prevent updating at the same time
        sleepAtLeastMillis(1);

        cache2.put("key1", "LatestUpdatedValue");
        cache2.get("key1"); // Access to record

        cache2.put("key2", "value2");
        cache2.get("key1"); // Access to record

        // Prevent updating at the same time
        sleepAtLeastMillis(1);

        cache1.put("key2", "LatestUpdatedValue2");
        cache1.get("key2"); // Access to record

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        Cache cacheTest = cacheManager1.getCache(cacheName);
        assertEquals("LatestUpdatedValue", cacheTest.get("key1"));
        assertEquals("LatestUpdatedValue2", cacheTest.get("key2"));
    }

    @Test
    public void testHigherHitsCacheMergePolicy() {
        String cacheName = randomMapName();
        Config config = newConfig();
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

        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(h1);
        CachingProvider cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(h2);

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig cacheConfig = newCacheConfig(cacheName, HigherHitsCacheMergePolicy.class.getName());

        Cache cache1 = cacheManager1.createCache(cacheName, cacheConfig);
        Cache cache2 = cacheManager2.createCache(cacheName, cacheConfig);

        cache1.put("key1", "higherHitsValue");
        cache1.put("key2", "value2");

        // Increase hits number
        cache1.get("key1");
        cache1.get("key1");

        cache2.put("key1", "value1");
        cache2.put("key2", "higherHitsValue2");

        // Increase hits number
        cache2.get("key2");
        cache2.get("key2");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        Cache cacheTest = cacheManager2.getCache(cacheName);
        assertEquals("higherHitsValue", cacheTest.get("key1"));
        assertEquals("higherHitsValue2", cacheTest.get("key2"));
    }

    @Test
    public void testPutIfAbsentCacheMergePolicy() {
        String cacheName = randomMapName();
        Config config = newConfig();
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

        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(h1);
        CachingProvider cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(h2);

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig cacheConfig = newCacheConfig(cacheName, PutIfAbsentCacheMergePolicy.class.getName());

        Cache cache1 = cacheManager1.createCache(cacheName, cacheConfig);
        Cache cache2 = cacheManager2.createCache(cacheName, cacheConfig);

        cache1.put("key1", "PutIfAbsentValue1");

        cache2.put("key1", "value");
        cache2.put("key2", "PutIfAbsentValue2");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        Cache cacheTest = cacheManager2.getCache(cacheName);
        assertEquals("PutIfAbsentValue1", cacheTest.get("key1"));
        assertEquals("PutIfAbsentValue2", cacheTest.get("key2"));
    }

    @Test
    public void testPassThroughCacheMergePolicy() {
        String cacheName = randomMapName();
        Config config = newConfig();
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

        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(h1);
        CachingProvider cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(h2);

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig cacheConfig = newCacheConfig(cacheName, PassThroughCacheMergePolicy.class.getName());

        Cache cache1 = cacheManager1.createCache(cacheName, cacheConfig);
        Cache cache2 = cacheManager2.createCache(cacheName, cacheConfig);

        String key = generateKeyOwnedBy(h1);
        cache1.put(key, "value");

        cache2.put(key, "passThroughValue");

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        Cache cacheTest = cacheManager2.getCache(cacheName);
        assertEquals("passThroughValue", cacheTest.get(key));
    }

    @Test
    public void testCustomCacheMergePolicy() {
        String cacheName = randomMapName();
        Config config = newConfig();
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

        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(h1);
        CachingProvider cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(h2);

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig cacheConfig = newCacheConfig(cacheName, CustomCacheMergePolicy.class.getName());

        Cache cache1 = cacheManager1.createCache(cacheName, cacheConfig);
        Cache cache2 = cacheManager2.createCache(cacheName, cacheConfig);

        String key = generateKeyOwnedBy(h1);
        cache1.put(key, "value");

        cache2.put(key,Integer.valueOf(1));

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        Cache cacheTest = cacheManager2.getCache(cacheName);
        assertNotNull(cacheTest.get(key));
        assertTrue(cacheTest.get(key) instanceof Integer);
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config.getGroupConfig().setName(generateRandomString(10));
        return config;
    }

    private CacheConfig newCacheConfig(String cacheName, String mergePolicy) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setMergePolicy(mergePolicy);
        return cacheConfig;
    }

    private static class TestLifeCycleListener implements LifecycleListener {

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

    private static class TestMemberShipListener implements MembershipListener {

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

    private static class CustomCacheMergePolicy implements OriginalTypeAwareCacheMergePolicy {

        @Override
        public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
            if (mergingEntry.getValue() instanceof Integer) {
                return mergingEntry.getValue();
            }
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }

    }

}
