package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cache.CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
    public class CachePartitionLostListenerStressTest
        extends AbstractPartitionLostListenerTest {

    protected int getNodeCount() {
        return 5;
    }

    protected int getCacheEntryCount() {
        return 10000;
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when1NodeCrashed_withoutData()
            throws InterruptedException {
        testCachePartitionLostListener(1, false);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when1NodeCrashed_withData()
            throws InterruptedException {
        testCachePartitionLostListener(1, true);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when2NodesCrashed_withoutData()
            throws InterruptedException {
        testCachePartitionLostListener(2, false);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when2NodesCrashed_withData()
            throws InterruptedException {
        testCachePartitionLostListener(2, true);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when3NodesCrashed_withoutData()
            throws InterruptedException {
        testCachePartitionLostListener(3, false);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when3NodesCrashed_withData()
            throws InterruptedException {
        testCachePartitionLostListener(3, true);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when4NodesCrashed_withoutData()
            throws InterruptedException {
        testCachePartitionLostListener(4, false);
    }

    @Test
    public void test_cachePartitionLostListenerInvoked_when4NodesCrashed_withData()
            throws InterruptedException {
        testCachePartitionLostListener(4, true);
    }

    private void testCachePartitionLostListener(final int numberOfNodesToCrash, final boolean withData) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        final HazelcastInstance instance = survivingInstances.get(0);
        final HazelcastServerCachingProvider cachingProvider = createCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final List<EventCollectingCachePartitionLostListener> listeners = registerListeners(cacheManager);

        if (withData) {
            for (int i = 0; i < getNodeCount(); i++) {
                final Cache<Integer, Integer> cache = cacheManager.getCache(getIthCacheName(i));
                for (int j = 0; j < getCacheEntryCount(); j++) {
                    cache.put(j, j);
                }
            }
        }

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        final Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        for (int i = 0; i < getNodeCount(); i++) {
            assertListenerInvocationsEventually(numberOfNodesToCrash, log, survivingPartitions, listeners.get(i), i);
        }

        for (int i = 0; i < getNodeCount(); i++) {
            cacheManager.destroyCache(getIthCacheName(i));
        }
        cacheManager.close();
        cachingProvider.close();
    }

    private void assertListenerInvocationsEventually(final int numberOfNodesToCrash, final String log,
                                                     final Map<Integer, Integer> survivingPartitions,
                                                     final EventCollectingCachePartitionLostListener listener, final int index) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    final String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }

    private void assertLostPartitions(final String log, final CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener listener,
                                      final Map<Integer, Integer> survivingPartitions) {
        final List<CachePartitionLostEvent> events = listener.getEvents();

        assertFalse(survivingPartitions.isEmpty());

        for (CachePartitionLostEvent event : events) {
            final int failedPartitionId = event.getPartitionId();
            final Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                final String message =
                        log + ", PartitionId: " + failedPartitionId + " SurvivingReplicaIndex: " + survivingReplicaIndex
                                + " Cache Name: " + event.getName();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }

    private List<CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener> registerListeners(final CacheManager cacheManager) {
        final CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        final List<EventCollectingCachePartitionLostListener> listeners = new ArrayList<EventCollectingCachePartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(i);
            listeners.add(listener);
            config.setBackupCount(i);
            final Cache<Integer, Integer> cache = cacheManager.createCache(getIthCacheName(i), config);
            final ICache iCache = cache.unwrap(ICache.class);
            iCache.addPartitionLostListener(listener);
        }
        return listeners;
    }

}