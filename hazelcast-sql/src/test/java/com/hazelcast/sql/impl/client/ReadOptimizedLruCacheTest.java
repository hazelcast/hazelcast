/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.client;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadOptimizedLruCacheTest {

    @Test
    public void test() {
        ReadOptimizedLruCache<Integer, Integer> c = new ReadOptimizedLruCache<>(2, 3);
        c.put(42, 42);
        sleepMillis(20); // a little sleep to ensure the lastUsed timestamps are different even on a very imprecise clock
        c.put(43, 43);
        sleepMillis(20);
        c.put(44, 44);
        assertEquals(3, c.cache.size());
        sleepMillis(20);
        c.put(45, 45);
        assertEquals(2, c.cache.size());
        assertEquals(Integer.valueOf(44), c.cache.get(44).value);
        assertEquals(Integer.valueOf(45), c.cache.get(45).value);

        sleepMillis(20);
        c.put(46, 46);
        sleepMillis(20);
        c.get(44); // access makes the value the least recently used one

        c.put(47, 47);
        assertEquals(2, c.cache.size());
        assertEquals(Integer.valueOf(44), c.cache.get(44).value);
        assertEquals(Integer.valueOf(47), c.cache.get(47).value);
    }
}
