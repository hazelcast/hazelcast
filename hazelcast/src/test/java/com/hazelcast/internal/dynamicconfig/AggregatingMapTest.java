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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregatingMapTest extends HazelcastTestSupport {

    @Test
    public void givenKeyExistInTheFirstMap_whenGet_theHit() {
        String key = "key";
        String value = "value";
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put(key, value);

        Map<String, String> map2 = new HashMap<String, String>();

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        assertEquals(value, aggregatingMap.get(key));
    }

    @Test
    public void givenKeyExistInTheSecondMap_whenGet_theHit() {
        String key = "key";
        String value = "value";
        Map<String, String> map1 = new HashMap<String, String>();

        Map<String, String> map2 = new HashMap<String, String>();
        map2.put(key, value);

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        assertEquals(value, aggregatingMap.get(key));
    }

    @Test
    public void whenBothMapsAreNull_thenSizeIsEmpty() {
        Map<String, String> aggregatingMap = AggregatingMap.aggregate(null, null);
        assertTrue(aggregatingMap.isEmpty());
    }

    @Test
    public void testAggregatingSize() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");

        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        assertEquals(2, aggregatingMap.size());
    }

    @Test
    public void testContainsKey() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");

        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        assertTrue(aggregatingMap.containsKey("key1"));
        assertTrue(aggregatingMap.containsKey("key2"));
        assertFalse(aggregatingMap.containsKey("key3"));
    }

    @Test
    public void testContainsValue() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");

        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        assertTrue(aggregatingMap.containsValue("value1"));
        assertTrue(aggregatingMap.containsValue("value2"));
        assertFalse(aggregatingMap.containsValue("value3"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutThrowUOE() {
        Map<String, String> map1 = new HashMap<String, String>();
        Map<String, String> map2 = new HashMap<String, String>();

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        aggregatingMap.put("key", "value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClearThrowUOE() {
        Map<String, String> map1 = new HashMap<String, String>();
        Map<String, String> map2 = new HashMap<String, String>();

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        aggregatingMap.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveThrowUOE() {
        Map<String, String> map1 = new HashMap<String, String>();
        Map<String, String> map2 = new HashMap<String, String>();

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        aggregatingMap.remove("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutAllThrowUOE() {
        Map<String, String> map1 = new HashMap<String, String>();
        Map<String, String> map2 = new HashMap<String, String>();

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        Map<String, String> tempMap = new HashMap<String, String>();
        tempMap.put("key", "value");
        aggregatingMap.putAll(tempMap);
    }

    @Test
    public void testKeys_hasKeyFromBothMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        Set<String> keySet = aggregatingMap.keySet();

        assertEquals(2, keySet.size());
        assertThat(keySet, containsInAnyOrder("key1", "key2"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKeys_isUnmodifiable() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        Set<String> keySet = aggregatingMap.keySet();

        keySet.remove("key1");
    }

    @Test
    public void testValues_hasValuesFromBothMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        Collection<String> values = aggregatingMap.values();

        assertEquals(2, values.size());
        assertThat(values, contains("value", "value"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValues_isUnmodifiable() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        Collection<String> values = aggregatingMap.values();

        values.remove("value");
    }

    @Test
    public void testEntrySet_hasKeyFromBothMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        for (Map.Entry<String, String> entry : aggregatingMap.entrySet()) {
            String key = entry.getKey();
            if (key.equals("key1")) {
                assertEquals("value1", entry.getValue());
            } else if (key.equals("key2")) {
                assertEquals("value2", entry.getValue());
            } else {
                fail();
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySet_isUnmodifiable() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("key1", "value1");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("key2", "value2");

        Map<String, String> aggregatingMap = AggregatingMap.aggregate(map1, map2);
        aggregatingMap.remove("entry");
    }
}
