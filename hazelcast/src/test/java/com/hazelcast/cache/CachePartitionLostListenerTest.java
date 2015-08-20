package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachePartitionLostListenerTest
        extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    public static class EventCollectingCachePartitionLostListener
            implements CachePartitionLostListener {

        private final List<CachePartitionLostEvent> events = Collections.synchronizedList(new LinkedList<CachePartitionLostEvent>());

        private final int backupCount;

        public EventCollectingCachePartitionLostListener(int backupCount) {
            this.backupCount = backupCount;
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
            this.events.add(event);
        }

        public List<CachePartitionLostEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<CachePartitionLostEvent>(events);
            }
        }

        public int getBackupCount() {
            return backupCount;
        }
    }

    @Test
    public void test_partitionLostListenerInvoked(){
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        final HazelcastInstance instance = instances.get(0);
        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        final HazelcastServerCachingProvider cachingProvider = createCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        final Cache<Integer, String> cache = cacheManager.createCache(getIthCacheName(0), config);
        final ICache iCache = cache.unwrap(ICache.class);

        iCache.addPartitionLostListener(listener);

        final InternalPartitionLostEvent internalEvent = new InternalPartitionLostEvent(1, 1, null);
        final CacheService cacheService = getNode(instance).getNodeEngine().getService(CacheService.SERVICE_NAME);
        cacheService.onPartitionLost(internalEvent);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final List<CachePartitionLostEvent> events = listener.getEvents();
                assertEquals(1, events.size());
                final CachePartitionLostEvent event = events.get(0);
                assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
            }
        });

        cacheManager.destroyCache(getIthCacheName(0));
        cacheManager.close();
        cachingProvider.close();
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {

        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(2);
        final HazelcastInstance survivingInstance = instances.get(0);
        final HazelcastInstance terminatingInstance = instances.get(1);
        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        final HazelcastServerCachingProvider cachingProvider = createCachingProvider(survivingInstance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setBackupCount(0);
        final Cache<Integer, String> cache = cacheManager.createCache(getIthCacheName(0), config);
        final ICache iCache = cache.unwrap(ICache.class);

        iCache.addPartitionLostListener(listener);

        final Set<Integer> survivingPartitionIds = new HashSet<Integer>();
        final Node survivingNode = getNode(survivingInstance);
        final Address survivingAddress = survivingNode.getThisAddress();

        for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final List<CachePartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                for (CachePartitionLostEvent event : events) {
                    assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                }
            }
        });

        cacheManager.destroyCache(getIthCacheName(0));
        cacheManager.close();
        cachingProvider.close();

    }

}
