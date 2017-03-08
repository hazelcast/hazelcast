package com.hazelcast.internal.eviction;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SamplingEvictionStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Matchers;

import java.util.Collections;

import static com.hazelcast.internal.eviction.EvictionListener.NO_LISTENER;
import static com.hazelcast.internal.eviction.EvictionChecker.EVICT_ALWAYS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EvictionStrategyTest<K, V extends Evictable, S extends SampleableEvictableStore<K, V>> extends HazelcastTestSupport {

    private HazelcastInstance instance;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
    }

    private class SimpleEvictionCandidate implements EvictionCandidate<K, V> {

        private K key;
        private V value;

        private SimpleEvictionCandidate(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getAccessor() {
            return key;
        }

        @Override
        public V getEvictable() {
            return value;
        }

        @Override
        public Object getKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCreationTime() {
            return getEvictable().getCreationTime();
        }

        @Override
        public long getLastAccessTime() {
            return getEvictable().getLastAccessTime();
        }

        @Override
        public long getAccessHit() {
            return getEvictable().getAccessHit();
        }
    }

    @Test
    public void evictionPolicySuccessfullyEvaluatedOnSamplingBasedEvictionStrategy() {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_COUNT = 1;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;

        Node node = TestUtil.getNode(instance);

        SerializationService serializationService = node.getSerializationService();
        ICacheService cacheService = node.getNodeEngine().getService(ICacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext("MyCache");

        EvictionConfiguration evictionConfig = new EvictionConfiguration() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return EvictionStrategyType.SAMPLING_BASED_EVICTION;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return null;
            }

            @Override
            public String getComparatorClassName() {
                return null;
            }

            @Override
            public EvictionPolicyComparator getComparator() {
                return null;
            }
        };
        SamplingEvictionStrategy<K, V, S> evictionStrategy = SamplingEvictionStrategy.INSTANCE;
        CacheRecordHashMap cacheRecordMap = new CacheRecordHashMap(serializationService, 1000, cacheContext);
        CacheObjectRecord expectedEvictedRecord = null;
        Data expectedData = null;

        for (int i = 0; i < RECORD_COUNT; i++) {
            CacheObjectRecord record = new CacheObjectRecord(i, System.currentTimeMillis(), Long.MAX_VALUE);
            Data data = serializationService.toData(i);
            cacheRecordMap.put(data, record);
            if (i == EXPECTED_EVICTED_RECORD_VALUE) {
                expectedEvictedRecord = record;
                expectedData = data;
            }
        }

        assertNotNull(expectedEvictedRecord);
        assertNotNull(expectedData);

        final SimpleEvictionCandidate evictionCandidate
                = new SimpleEvictionCandidate((K) expectedData, (V) expectedEvictedRecord);
        // we are testing "EvictionStrategy" in this test, so we mock "EvictionPolicyEvaluator" (it's tested in another test)
        EvictionPolicyEvaluator evictionPolicyEvaluator = mock(EvictionPolicyEvaluator.class);
        when(evictionPolicyEvaluator.evaluate(Matchers.any(Iterable.class))).
                thenReturn(Collections.singleton(evictionCandidate));
        when(evictionPolicyEvaluator.getEvictionPolicyComparator()).thenReturn(null);

        assertEquals(RECORD_COUNT, cacheRecordMap.size());
        assertTrue(cacheRecordMap.containsKey(expectedData));
        assertTrue(cacheRecordMap.containsValue(expectedEvictedRecord));

        int evictedCount = evictionStrategy.evict((S) cacheRecordMap, evictionPolicyEvaluator, EVICT_ALWAYS, NO_LISTENER);
        assertEquals(EXPECTED_EVICTED_COUNT, evictedCount);
        assertEquals(RECORD_COUNT - EXPECTED_EVICTED_COUNT, cacheRecordMap.size());
        assertFalse(cacheRecordMap.containsKey(expectedData));
        assertFalse(cacheRecordMap.containsValue(expectedEvictedRecord));
    }
}
