package com.hazelcast.cache.eviction;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionConfig;
import com.hazelcast.cache.impl.eviction.EvictionPolicyEvaluator;
import com.hazelcast.cache.impl.eviction.EvictionPolicyEvaluatorProvider;
import com.hazelcast.cache.impl.eviction.EvictionPolicyType;
import com.hazelcast.cache.impl.eviction.EvictionStrategyType;
import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EvictionPolicyEvaluatorTest extends HazelcastTestSupport {

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
    public void evictionPolicyLRUSuccessfullyEvaluated() {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;

        EvictionConfig evictionConfig = new EvictionConfig() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return null;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return EvictionPolicyType.LRU;
            }
        };
        EvictionPolicyEvaluator evictionPolicyEvaluator =
                EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(evictionConfig);
        List<EvictionCandidate<Integer, CacheObjectRecord>> records =
                new ArrayList<EvictionCandidate<Integer, CacheObjectRecord>>();

        long baseTime = System.currentTimeMillis();

        for (int i = 0; i < RECORD_COUNT; i++) {
            long creationTime = baseTime + (i * 100);
            CacheObjectRecord record = new CacheObjectRecord(i, creationTime, Long.MAX_VALUE);
            if (i == EXPECTED_EVICTED_RECORD_VALUE) {
                // The record in the middle will be minimum access time.
                // So, it will be selected for eviction
                record.setAccessTime(baseTime - 1000);
            } else {
                record.setAccessTime(creationTime + 1000);
            }
            records.add(new SimpleEvictionCandidate<Integer, CacheObjectRecord>(i, record));
        }

        Iterable<EvictionCandidate<Integer, CacheObjectRecord>> evictedRecords =
                evictionPolicyEvaluator.evaluate(records);

        assertNotNull(evictedRecords);

        Iterator<EvictionCandidate<Integer, CacheObjectRecord>> evictedRecordsIterator = evictedRecords.iterator();
        assertTrue(evictedRecordsIterator.hasNext());

        EvictionCandidate<Integer, CacheObjectRecord> candidateEvictedRecord = evictedRecordsIterator.next();
        assertNotNull(candidateEvictedRecord);
        assertFalse(evictedRecordsIterator.hasNext());

        CacheObjectRecord evictedRecord = candidateEvictedRecord.getEvictable();
        assertNotNull(evictedRecord);
        assertEquals(EXPECTED_EVICTED_RECORD_VALUE, evictedRecord.getValue());
    }

    @Test
    public void evictionPolicyLFUSuccessfullyEvaluated() {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;

        EvictionConfig evictionConfig = new EvictionConfig() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return null;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return EvictionPolicyType.LFU;
            }
        };
        EvictionPolicyEvaluator evictionPolicyEvaluator =
                EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(evictionConfig);
        List<EvictionCandidate<Integer, CacheObjectRecord>> records =
                new ArrayList<EvictionCandidate<Integer, CacheObjectRecord>>();

        for (int i = 0; i < RECORD_COUNT; i++) {
            CacheObjectRecord record = new CacheObjectRecord(i, System.currentTimeMillis(), Long.MAX_VALUE);
            if (i == EXPECTED_EVICTED_RECORD_VALUE) {
                // The record in the middle will be minimum access hit.
                // So, it will be selected for eviction
                record.setAccessHit(0);
            } else {
                record.setAccessHit(i + 1);
            }
            records.add(new SimpleEvictionCandidate<Integer, CacheObjectRecord>(i, record));
        }

        Iterable<EvictionCandidate<Integer, CacheObjectRecord>> evictedRecords =
                evictionPolicyEvaluator.evaluate(records);

        assertNotNull(evictedRecords);

        Iterator<EvictionCandidate<Integer, CacheObjectRecord>> evictedRecordsIterator = evictedRecords.iterator();
        assertTrue(evictedRecordsIterator.hasNext());

        EvictionCandidate<Integer, CacheObjectRecord> candidateEvictedRecord = evictedRecordsIterator.next();
        assertNotNull(candidateEvictedRecord);
        assertFalse(evictedRecordsIterator.hasNext());

        CacheObjectRecord evictedRecord = candidateEvictedRecord.getEvictable();
        assertNotNull(evictedRecord);
        assertEquals(EXPECTED_EVICTED_RECORD_VALUE, evictedRecord.getValue());
    }

}
