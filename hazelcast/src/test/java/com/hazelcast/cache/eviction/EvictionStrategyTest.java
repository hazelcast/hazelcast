package com.hazelcast.cache.eviction;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionChecker;
import com.hazelcast.cache.impl.eviction.EvictionConfig;
import com.hazelcast.cache.impl.eviction.EvictionPolicyEvaluator;
import com.hazelcast.cache.impl.eviction.EvictionPolicyType;
import com.hazelcast.cache.impl.eviction.EvictionStrategy;
import com.hazelcast.cache.impl.eviction.EvictionStrategyProvider;
import com.hazelcast.cache.impl.eviction.EvictionStrategyType;
import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EvictionStrategyTest extends HazelcastTestSupport {

    private HazelcastInstance instance;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
    }

    private class SimpleEvictionCandidate<K, V extends Evictable> implements EvictionCandidate<K, V> {

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

    }

    @Test
    public void evictionPolicySuccessfullyEvaluatedOnSamplingBasedEvictionStrategy() {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_COUNT = 1;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;

        Node node = TestUtil.getNode(instance);

        SerializationService serializationService = node.getSerializationService();

        EvictionConfig evictionConfig = new EvictionConfig() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return EvictionStrategyType.SAMPLING_BASED_EVICTION;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return null;
            }
        };
        EvictionStrategy evictionStrategy =
                EvictionStrategyProvider.getEvictionStrategy(evictionConfig);
        CacheRecordHashMap cacheRecordMap = new CacheRecordHashMap(1000);
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

        final SimpleEvictionCandidate evictionCandidate =
                new SimpleEvictionCandidate(expectedData, expectedEvictedRecord);
        // Mock "EvictionPolicyEvaluator", since we are testing it in other tests.
        // In this test, we are testing "EvictionStrategy".
        EvictionPolicyEvaluator evictionPolicyEvaluator =
                new EvictionPolicyEvaluator() {
                    @Override
                    public Iterable<SimpleEvictionCandidate> evaluate(Iterable evictionCandidates) {
                        return Collections.singleton(evictionCandidate);
                    }
                };

        assertEquals(RECORD_COUNT, cacheRecordMap.size());
        assertTrue(cacheRecordMap.containsKey(expectedData));
        assertTrue(cacheRecordMap.containsValue(expectedEvictedRecord));

        int evictedCount = evictionStrategy.evict(cacheRecordMap, evictionPolicyEvaluator,
                EvictionChecker.EVICT_ALWAYS);
        assertEquals(EXPECTED_EVICTED_COUNT, evictedCount);
        assertEquals(RECORD_COUNT - EXPECTED_EVICTED_COUNT, cacheRecordMap.size());
        assertFalse(cacheRecordMap.containsKey(expectedData));
        assertFalse(cacheRecordMap.containsValue(expectedEvictedRecord));
    }

}
