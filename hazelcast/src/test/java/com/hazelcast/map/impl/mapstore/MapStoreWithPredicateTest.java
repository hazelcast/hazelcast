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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapStoreWithPredicateTest extends AbstractMapStoreTest {

    @Test
    public void testKeySetWithPredicate_checksMapStoreLoad() {
        EventBasedMapStore<String, Integer> testMapStore = new EventBasedMapStore<String, Integer>();

        Map<String, Integer> mapForStore = new HashMap<String, Integer>();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Set expected = map.keySet(Predicates.greaterThan("value", 1));
                assertEquals(3, expected.size());
                assertContains(expected, "key1");
                assertContains(expected, "key2");
                assertContains(expected, "key3");
            }
        });
    }

    @Test
    public void testValuesWithPredicate_checksMapStoreLoad() {
        EventBasedMapStore<String, Integer> testMapStore = new EventBasedMapStore<String, Integer>();

        Map<String, Integer> mapForStore = new HashMap<String, Integer>();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final Collection values = map.values(Predicates.greaterThan("value", 1));
                assertEquals(3, values.size());
                assertContains(values, 17);
                assertContains(values, 37);
                assertContains(values, 47);
            }
        });
    }

    @Test
    public void testEntrySetWithPredicate_checksMapStoreLoad() {
        EventBasedMapStore<String, Integer> testMapStore = new EventBasedMapStore<String, Integer>();

        Map<String, Integer> mapForStore = new HashMap<String, Integer>();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<String, Integer> map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final Set<Map.Entry<String, Integer>> entrySet = map.entrySet(Predicates.greaterThan("value", 1));
                assertEquals(3, entrySet.size());
            }
        });
    }
}
