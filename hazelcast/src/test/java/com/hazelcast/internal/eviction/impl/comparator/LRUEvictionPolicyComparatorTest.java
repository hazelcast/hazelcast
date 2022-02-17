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

package com.hazelcast.internal.eviction.impl.comparator;

import com.hazelcast.spi.eviction.EvictableEntryView;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LRUEvictionPolicyComparatorTest {

    private static final long NOW = 20;

    @Test
    public void lru_comparator_does_not_prematurely_select_newly_created_entries() {
        // 0. Entries to sort
        List<TestEntryView> givenEntries = new LinkedList<>();
        givenEntries.add(new TestEntryView(1, 1, 0));
        givenEntries.add(new TestEntryView(2, 2, 3));
        givenEntries.add(new TestEntryView(3, 2, 0));
        givenEntries.add(new TestEntryView(4, 4, 4));
        givenEntries.add(new TestEntryView(5, 5, 20));
        givenEntries.add(new TestEntryView(6, 6, 6));
        givenEntries.add(new TestEntryView(7, 7, 0));
        givenEntries.add(new TestEntryView(8, 9, 15));
        givenEntries.add(new TestEntryView(9, 10, 10));
        givenEntries.add(new TestEntryView(10, 10, 0));

        // 1. Create expected list of ordered elements by
        // sorting entries based on their idle-times. Longest
        // idle time must be the first element of the list.
        List<TestEntryView> descOrderByIdleTimes = new LinkedList<>(givenEntries);
        Collections.sort(descOrderByIdleTimes, (o1, o2) -> -Long.compare(idleTime(o1), idleTime(o2)));

        // 2. Then sort given entries by using LRU eviction comparator.
        Collections.sort(givenEntries, (o1, o2) -> LRUEvictionPolicyComparator.INSTANCE.compare(o1, o2));

        // 3. Check both lists are equal
        assertEquals(descOrderByIdleTimes, givenEntries);
    }

    private static long idleTime(EvictableEntryView entryView) {
        return NOW - Math.max(entryView.getCreationTime(), entryView.getLastAccessTime());
    }

    private static class TestEntryView implements EvictableEntryView {
        private long id;
        private long creationTime;
        private long lastAccessTime;

        TestEntryView(long id, long creationTime, long lastAccessTime) {
            this.id = id;
            this.creationTime = creationTime;
            this.lastAccessTime = lastAccessTime;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestEntryView that = (TestEntryView) o;

            if (id != that.id) {
                return false;
            }
            if (creationTime != that.creationTime) {
                return false;
            }
            return lastAccessTime == that.lastAccessTime;
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
            result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TestEntryView{"
                    + "id=" + id
                    + ", now=" + NOW
                    + ", creationTime=" + creationTime
                    + ", lastAccessTime=" + lastAccessTime
                    + ", idleTime=" + idleTime(this) + '}';
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
        public long getHits() {
            throw new UnsupportedOperationException();
        }
    }
}
