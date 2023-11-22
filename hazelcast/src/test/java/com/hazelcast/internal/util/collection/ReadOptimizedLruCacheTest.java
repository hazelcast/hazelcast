/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.util.Comparator.comparingLong;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadOptimizedLruCacheTest {
    private ReadOptimizedLruCache<Integer, Integer> cache;

    @Before
    public void init() {
        cache = new ReadOptimizedLruCache<>(2, 3);
        put(1, 1);
        put(2, 2);
        put(3, 3);
    }

    @Test
    public void test_cleanup() {
        assertEntries(entry(1, 1), entry(2, 2), entry(3, 3));

        put(4, 4);
        assertEntries(entry(3, 3), entry(4, 4));
    }

    @Test
    public void test_changeAccessTime() {
        cache.get(1); // Access makes it the most recently used one
        assertEntries(entry(2, 2), entry(3, 3), entry(1, 1));

        put(4, 4);
        assertEntries(entry(1, 1), entry(4, 4));
    }

    @Test
    public void test_computeIfAbsent() {
        cache.computeIfAbsent(1, k -> 1); // Access makes it the most recently used one
        assertEntries(entry(2, 2), entry(3, 3), entry(1, 1));

        cache.computeIfAbsent(4, k -> 4);
        assertEntries(entry(1, 1), entry(4, 4));

        sleepMillis(20);
        cache.computeIfAbsent(2, k -> 2);
        assertEntries(entry(1, 1), entry(4, 4), entry(2, 2));
    }

    @Test
    public void when_nullValueIsProvided_then_reject() {
        assertThatThrownBy(() -> put(1, null)).hasMessage("Null values are disallowed");
        assertEntries(entry(1, 1), entry(2, 2), entry(3, 3));

        assertThatCode(() -> cache.computeIfAbsent(1, k -> null)).doesNotThrowAnyException();
        assertEntries(entry(2, 2), entry(3, 3), entry(1, 1));

        assertThatThrownBy(() -> cache.computeIfAbsent(4, k -> null)).hasMessage("Null values are disallowed");
        assertEntries(entry(2, 2), entry(3, 3), entry(1, 1));
    }

    @Test
    public void test_remove_getOrDefault() {
        cache.remove(2);
        assertEntries(entry(1, 1), entry(3, 3));

        assertThat(cache.get(2)).isNull();
        assertThat(cache.getOrDefault(2, 2)).isEqualTo(2);
        assertThat(cache.getOrDefault(4, 4)).isEqualTo(4);
    }

    private void put(Integer key, Integer value) {
        // A little sleep to ensure the timestamps are different even on a very imprecise clock
        sleepMillis(20);
        cache.put(key, value);
    }

    @SafeVarargs
    private void assertEntries(Entry<Integer, Integer>... entries) {
        assertThat(cache.cache.entrySet().stream()
                .sorted(comparingLong(e -> e.getValue().timestamp))
                .map(e -> entry(e.getKey(), e.getValue().value))
        ).containsExactly(entries);
    }
}
