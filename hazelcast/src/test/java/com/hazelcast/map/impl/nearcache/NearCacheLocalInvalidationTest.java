/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheLocalInvalidationTest extends HazelcastTestSupport {

    private static final int INSTANCE_COUNT = 2;
    private static final int NUM_ITERATIONS = 1000;
    private static final long TIMEOUT = 100L;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final String MAP_NAME = NearCacheLocalInvalidationTest.class.getCanonicalName();

    private HazelcastInstance hzInstance1;
    private HazelcastInstance hzInstance2;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
        nearCacheConfig.setCacheLocalEntries(true);

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME + "*");
        mapConfig.setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        hzInstance1 = factory.newHazelcastInstance(config);
        hzInstance2 = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        hzInstance1.getLifecycleService().shutdown();
        hzInstance2.getLifecycleService().shutdown();
    }

    @Test
    public void testRemove() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "remove_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            String value2 = map.remove(key);
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertEquals(value, value2);
            assertNull(value3);
        }
    }

    @Test
    public void testDelete() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "delete_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.delete(key);
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testRemoveValue() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "removevalue_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.remove(key, value);
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testTryRemove() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "tryremove_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.tryRemove(key, TIMEOUT, TIME_UNIT);
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testRemoveAsync() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "removeasync_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            Future<String> future = map.removeAsync(key);
            String value2 = null;
            try {
                value2 = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertEquals(value, value2);
            assertNull(value3);
        }
    }

    // -------------------------------------------------------------------- put

    @Test
    public void testPut() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "put_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            String value1 = map.put(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testTryPut() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "tryput_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            map.tryPut(key, value, TIMEOUT, TIME_UNIT);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutIfAbsent() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "putifabsent_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            String value1 = map.putIfAbsent(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertEquals(value, value2);
            assertNull(value1);
        }
    }

    @Test
    public void testPutTransient() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "puttransient_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            map.putTransient(key, value, 0, TIME_UNIT);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutAsync() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "putasync_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            Future<String> future = map.putAsync(key, value);
            String value1 = null;
            try {
                value1 = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testSetAsync() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "setasync_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            Future<Void> future = map.setAsync(key, value);
            try {
                future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testEvict() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "evict_" + String.valueOf(k);
            String value = "merhaba-" + key;

            String value0 = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.evict(key);
            // here we _might_ still see the value
            String value3 = map.get(key);

            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testSet() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "set_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            map.set(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testReplace() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "replace_" + String.valueOf(k);
            String value = "merhaba-" + key;
            String valueNew = "merhaba-new" + key;

            map.put(key, value);
            // this brings the CACHED_AS_NULL into the Near Cache
            String value0 = map.get(key);
            map.replace(key, valueNew);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNotNull(value0);
            assertEquals(valueNew, value2);
        }
    }

    @Test
    public void testExecuteOnKey() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "executeOnKey_" + String.valueOf(k);
            // bring null local cache
            String expectedNull = map.get(key);
            assertNull(expectedNull);
            Object o = map.executeOnKey(key, new WritingEntryProcessor());
            String newValue = (String) o;
            String value2 = map.get(key);
            assertEquals(newValue, value2);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteOnKeys() {
        IMap<String, String> map = hzInstance1.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < NUM_ITERATIONS; k++) {
            String key = "executeOnKeys_" + String.valueOf(k);
            // bring null to local cache
            String expectedNull = map.get(key);
            assertNull(expectedNull);
            HashSet<String> keys = new HashSet<String>();
            keys.add(key);
            Map<String, String> result = (Map) map.executeOnKeys(keys, new WritingEntryProcessor());
            for (Map.Entry<String, String> e : result.entrySet()) {
                String newValue = e.getValue();
                String cachedValue = map.get(e.getKey());
                assertEquals(newValue, cachedValue);
            }
        }
    }

    private static String getMapName() {
        return randomMapName(MAP_NAME);
    }

    /**
     * An entry processor which writes (changes the value and calls Entry.setValue()).
     */
    public static class WritingEntryProcessor extends AbstractEntryProcessor<String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public Object process(Map.Entry<String, String> entry) {
            entry.setValue("new value");
            return "new value";
        }

    }
}
