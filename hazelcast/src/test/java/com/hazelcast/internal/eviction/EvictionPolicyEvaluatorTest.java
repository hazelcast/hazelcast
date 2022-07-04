/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EvictionPolicyEvaluatorTest extends HazelcastTestSupport {

    private final class SimpleEvictionCandidate<K, V extends Evictable> implements EvictionCandidate<K, V> {

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
        public long getHits() {
            return getEvictable().getHits();
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
        final int recordCount = 100;
        final int expectedEvictedRecordValue = recordCount / 2;
        final int expectedExpiredRecordValue = useExpiredEntry ? recordCount / 4 : -1;

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
            public String getComparatorClassName() {
                return null;
            }

            @Override
            public EvictionPolicyComparator getComparator() {
                return null;
            }
        };
        EvictionPolicyEvaluator evictionPolicyEvaluator = getEvictionPolicyEvaluator(evictionConfig, null);
        List<EvictionCandidate<Integer, CacheObjectRecord>> records
                = new ArrayList<EvictionCandidate<Integer, CacheObjectRecord>>();

        long baseTime = System.currentTimeMillis();
        long minCreationTime = -1;
        for (int i = 0; i < recordCount; i++) {
            long creationTime = baseTime + (i * 100);
            minCreationTime = minCreationTime == -1 ? creationTime : Math.min(creationTime, minCreationTime);

            CacheObjectRecord record = new CacheObjectRecord(i, creationTime, Long.MAX_VALUE);
            if (i == expectedEvictedRecordValue) {
                // The record in the middle will be minimum access
                // time. So, it will be selected for eviction
                // (set creation-time also to keep this condition
                // true --> creation-time <= last-access-time)
                record.setCreationTime(minCreationTime);
                record.setLastAccessTime(minCreationTime + 1);
            } else if (i == expectedExpiredRecordValue) {
                record.setExpirationTime(System.currentTimeMillis());
            } else {
                record.setLastAccessTime(creationTime + 1000);
            }
            records.add(new SimpleEvictionCandidate<>(i, record));
        }

        sleepAtLeastMillis(1);

        EvictionCandidate<Integer, CacheObjectRecord> evictionCandidate = evictionPolicyEvaluator.evaluate(records);
        assertNotNull(evictionCandidate);

        CacheObjectRecord evictedRecord = evictionCandidate.getEvictable();
        assertNotNull(evictedRecord);
        if (useExpiredEntry) {
            assertEquals(expectedExpiredRecordValue, evictedRecord.getValue());
        } else {
            assertEquals(expectedEvictedRecordValue, evictedRecord.getValue());
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
        final int recordCount = 100;
        final int expectedEvictedRecordValue = recordCount / 2;
        final int expectedExpiredRecordValue = useExpiredEntry ? recordCount / 4 : -1;

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
            public String getComparatorClassName() {
                return null;
            }

            @Override
            public EvictionPolicyComparator getComparator() {
                return null;
            }
        };
        EvictionPolicyEvaluator evictionPolicyEvaluator = getEvictionPolicyEvaluator(evictionConfig, null);
        List<EvictionCandidate<Integer, CacheObjectRecord>> records
                = new ArrayList<EvictionCandidate<Integer, CacheObjectRecord>>();

        for (int i = 0; i < recordCount; i++) {
            CacheObjectRecord record = new CacheObjectRecord(i, System.currentTimeMillis(), Long.MAX_VALUE);
            if (i == expectedEvictedRecordValue) {
                // The record in the middle will be minimum access hit.
                // So, it will be selected for eviction
                record.setHits(0);
            } else if (i == expectedExpiredRecordValue) {
                record.setExpirationTime(System.currentTimeMillis());
            } else {
                record.setHits(i + 1);
            }
            records.add(new SimpleEvictionCandidate<>(i, record));
        }

        EvictionCandidate<Integer, CacheObjectRecord> evictionCandidate = evictionPolicyEvaluator.evaluate(records);

        assertNotNull(evictionCandidate);

        CacheObjectRecord evictedRecord = evictionCandidate.getEvictable();
        assertNotNull(evictedRecord);
        if (useExpiredEntry) {
            assertEquals(expectedExpiredRecordValue, evictedRecord.getValue());
        } else {
            assertEquals(expectedEvictedRecordValue, evictedRecord.getValue());
        }
    }
}
