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

package com.hazelcast.internal.adapter;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionalMapDataStructureAdapterTest extends HazelcastTestSupport {

    private IMap<Integer, String> map;
    private TransactionalMapDataStructureAdapter<Integer, String> adapter;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        map = hazelcastInstance.getMap("TransactionalMapDataStructureAdapterTest");

        adapter = new TransactionalMapDataStructureAdapter<Integer, String>(hazelcastInstance,
                "TransactionalMapDataStructureAdapterTest");
    }

    @Test
    public void testSize() {
        map.put(23, "foo");
        map.put(42, "bar");

        assertEquals(2, adapter.size());
    }

    @Test
    public void testGet() {
        map.put(42, "foobar");

        String result = adapter.get(42);
        assertEquals("foobar", result);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testGetAsync() {
        adapter.getAsync(42);
    }

    @Test
    public void testSet() {
        adapter.set(23, "test");

        assertEquals("test", map.get(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testSetAsync() {
        adapter.setAsync(42, "value");
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testSetAsyncWithTtl() {
        adapter.setAsync(42, "value", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testSetAsyncWithExpiryPolicy() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        adapter.setAsync(42, "value", expiryPolicy);
    }

    @Test
    public void testPut() {
        map.put(42, "oldValue");

        String oldValue = adapter.put(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAsync() {
        adapter.putAsync(42, "newValue");
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAsyncWithTtl() {
        adapter.putAsync(42, "value", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAsyncWithExpiryPolicy() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        adapter.putAsync(42, "value", expiryPolicy);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutTransient() {
        adapter.putTransient(42, "value", 1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPutIfAbsent() {
        map.put(42, "oldValue");

        assertTrue(adapter.putIfAbsent(23, "newValue"));
        assertFalse(adapter.putIfAbsent(42, "newValue"));

        assertEquals("newValue", map.get(23));
        assertEquals("oldValue", map.get(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutIfAbsentAsync() {
        adapter.putIfAbsentAsync(23, "newValue");
    }

    @Test
    public void testReplace() {
        map.put(42, "oldValue");

        String oldValue = adapter.replace(42, "newValue");

        assertEquals("oldValue", oldValue);
        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testReplaceWithOldValue() {
        map.put(42, "oldValue");

        assertFalse(adapter.replace(42, "foobar", "newValue"));
        assertTrue(adapter.replace(42, "oldValue", "newValue"));

        assertEquals("newValue", map.get(42));
    }

    @Test
    public void testRemove() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        assertEquals("value-23", adapter.remove(23));
        assertFalse(map.containsKey(23));
    }

    @Test
    public void testRemoveWithOldValue() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        assertFalse(adapter.remove(23, "foobar"));
        assertTrue(adapter.remove(23, "value-23"));
        assertFalse(map.containsKey(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testRemoveAsync() {
        adapter.removeAsync(23);
    }

    @Test
    public void testDelete() {
        map.put(23, "value-23");
        assertTrue(map.containsKey(23));

        adapter.delete(23);
        assertFalse(map.containsKey(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testDeleteAsync() {
        adapter.deleteAsync(23);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testEvict() {
        adapter.evict(23);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testInvoke() {
        adapter.invoke(23, new ICacheReplaceEntryProcessor(), "value", "newValue");
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnKey() {
        adapter.executeOnKey(23, new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnKeys() {
        Set<Integer> keys = new HashSet<Integer>(singleton(23));
        adapter.executeOnKeys(keys, new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnEntries() {
        adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testExecuteOnEntriesWithPredicate() {
        adapter.executeOnEntries(new IMapReplaceEntryProcessor("value", "newValue"), Predicates.alwaysTrue());
    }

    @Test
    public void testContainsKey() {
        map.put(23, "value-23");

        assertTrue(adapter.containsKey(23));
        assertFalse(adapter.containsKey(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAll() {
        adapter.loadAll(true);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAllWithKeys() {
        adapter.loadAll(Collections.<Integer>emptySet(), true);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testLoadAllWithListener() {
        adapter.loadAll(Collections.<Integer>emptySet(), true, null);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testGetAll() {
        adapter.getAll(singleton(23));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testPutAll() {
        Map<Integer, String> expectedResult = new HashMap<Integer, String>();
        expectedResult.put(23, "value-23");
        expectedResult.put(42, "value-42");

        adapter.putAll(expectedResult);
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testRemoveAll() {
        adapter.removeAll();
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testRemoveAllWithKeys() {
        adapter.removeAll(singleton(42));
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testEvictAll() {
        adapter.evictAll();
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testInvokeAll() {
        Set<Integer> keys = new HashSet<Integer>(asList(23, 65, 88));
        adapter.invokeAll(keys, new ICacheReplaceEntryProcessor(), "value", "newValue");
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testClear() {
        adapter.clear();
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testClose() {
        adapter.close();
    }

    @Test
    public void testDestroy() {
        map.put(23, "foobar");

        adapter.destroy();

        assertTrue(map.isEmpty());
    }

    @Test(expected = MethodNotAvailableException.class)
    public void testGetLocalMapStats() {
        adapter.getLocalMapStats();
    }
}
