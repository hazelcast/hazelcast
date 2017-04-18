/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.eviction;

import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
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
    public void test_leastRecentlyAccessedEntry_isSelected_when_evictionPolicy_is_LRU() {
        test_evictionPolicyLRU(false);
    }

    @Test
    public void test_expiredEntry_hasMorePriority_than_leastRecentlyAccessedEntry_toBeEvicted_when_evictionPolicy_is_LRU() {
        test_evictionPolicyLRU(true);
    }

    private void test_evictionPolicyLRU(boolean useExpiredEntry) {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;
        final int EXPECTED_EXPIRED_RECORD_VALUE = useExpiredEntry ? RECORD_COUNT / 4 : -1;

        EvictionConfiguration evictionConfig = new EvictionConfiguration() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return null;
            }

            @Override
            public EvictionPolicy getEvictionPolicy() {
                return EvictionPolicy.LRU;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return EvictionPolicyType.LRU;
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
        EvictionPolicyEvaluator evictionPolicyEvaluator =
                EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(evictionConfig, null);
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
            } else if (i == EXPECTED_EXPIRED_RECORD_VALUE) {
                record.setExpirationTime(System.currentTimeMillis());
            } else {
                record.setAccessTime(creationTime + 1000);
            }
            records.add(new SimpleEvictionCandidate<Integer, CacheObjectRecord>(i, record));
        }

        sleepAtLeastMillis(1);

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
        if (useExpiredEntry) {
            assertEquals(EXPECTED_EXPIRED_RECORD_VALUE, evictedRecord.getValue());
        } else {
            assertEquals(EXPECTED_EVICTED_RECORD_VALUE, evictedRecord.getValue());
        }
    }

    @Test
    public void test_leastFrequentlyUsedEntry_isSelected_when_evictionPolicy_is_LFU() {
        test_evictionPolicyLFU(false);
    }

    @Test
    public void test_expiredEntry_hasMorePriority_than_leastFrequentlyUsedEntry_toBeEvicted_when_evictionPolicy_is_LFU() {
        test_evictionPolicyLFU(true);
    }

    private void test_evictionPolicyLFU(boolean useExpiredEntry) {
        final int RECORD_COUNT = 100;
        final int EXPECTED_EVICTED_RECORD_VALUE = RECORD_COUNT / 2;
        final int EXPECTED_EXPIRED_RECORD_VALUE = useExpiredEntry ? RECORD_COUNT / 4 : -1;

        EvictionConfiguration evictionConfig = new EvictionConfiguration() {
            @Override
            public EvictionStrategyType getEvictionStrategyType() {
                return null;
            }

            @Override
            public EvictionPolicy getEvictionPolicy() {
                return EvictionPolicy.LFU;
            }

            @Override
            public EvictionPolicyType getEvictionPolicyType() {
                return EvictionPolicyType.LFU;
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
        EvictionPolicyEvaluator evictionPolicyEvaluator =
                EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(evictionConfig, null);
        List<EvictionCandidate<Integer, CacheObjectRecord>> records =
                new ArrayList<EvictionCandidate<Integer, CacheObjectRecord>>();

        for (int i = 0; i < RECORD_COUNT; i++) {
            CacheObjectRecord record = new CacheObjectRecord(i, System.currentTimeMillis(), Long.MAX_VALUE);
            if (i == EXPECTED_EVICTED_RECORD_VALUE) {
                // The record in the middle will be minimum access hit.
                // So, it will be selected for eviction
                record.setAccessHit(0);
            } else if (i == EXPECTED_EXPIRED_RECORD_VALUE) {
                record.setExpirationTime(System.currentTimeMillis());
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
        if (useExpiredEntry) {
            assertEquals(EXPECTED_EXPIRED_RECORD_VALUE, evictedRecord.getValue());
        } else {
            assertEquals(EXPECTED_EVICTED_RECORD_VALUE, evictedRecord.getValue());
        }
    }

}
