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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NullQueryCacheTest extends HazelcastTestSupport {

    @Test
    public void testGet() throws Exception {
        assertNull(NULL_QUERY_CACHE.get(1));
    }

    @Test
    public void testContainsKey() throws Exception {
        assertFalse(NULL_QUERY_CACHE.containsKey(1));
    }

    @Test
    public void testContainsValue() throws Exception {
        assertFalse(NULL_QUERY_CACHE.containsValue(1));
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(NULL_QUERY_CACHE.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, NULL_QUERY_CACHE.size());
    }

    @Test
    public void testGetAll() throws Exception {
        assertNull(NULL_QUERY_CACHE.getAll(null));
    }

    @Test
    public void testKeySet() throws Exception {
        assertNull(NULL_QUERY_CACHE.keySet());
    }

    @Test
    public void testEntrySet() throws Exception {
        assertNull(NULL_QUERY_CACHE.entrySet());
    }

    @Test
    public void testValues() throws Exception {
        assertNull(NULL_QUERY_CACHE.values());
    }

    @Test
    public void testGetName() throws Exception {
        assertNull(NULL_QUERY_CACHE.getName());
    }
}
