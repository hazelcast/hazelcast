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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractMapNearCacheLocalInvalidationTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = randomMapName();

    private static final int INSTANCE_COUNT = 2;
    private static final int NUM_ITERATIONS = 1000;
    private static final long TIMEOUT = 100L;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Config config = createConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        hz = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
    }

    abstract Config createConfig();

    @Test
    public void testRemove() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "remove_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            String removedValue = map.remove(key);
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertEquals(value, removedValue);
            assertNull(value2);
        }
    }

    @Test
    public void testDelete() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "delete_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.delete(key);
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertNull(value2);
        }
    }

    @Test
    public void testRemoveValue() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "removevalue_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            assertTrue(map.remove(key, value));
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertNull(value2);
        }
    }

    @Test
    public void testTryRemove() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "tryremove_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.tryRemove(key, TIMEOUT, TIME_UNIT);
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertNull(value2);
        }
    }

    @Test
    public void testRemoveAsync() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "removeasync_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            Future<String> future = map.removeAsync(key).toCompletableFuture();
            String removedValue = null;
            try {
                removedValue = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertEquals(value, removedValue);
            assertNull(value2);
        }
    }

    @Test
    public void testPut() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "put_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            String oldValue = map.put(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertNull(oldValue);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testTryPut() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "tryput_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            map.tryPut(key, value, TIMEOUT, TIME_UNIT);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutIfAbsent() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "putifabsent_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            String oldValue = map.putIfAbsent(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertEquals(value, value2);
            assertNull(oldValue);
        }
    }

    @Test
    public void testPutTransient() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "puttransient_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            map.putTransient(key, value, 0, TIME_UNIT);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutAsync() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "putasync_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            Future<String> future = map.putAsync(key, value).toCompletableFuture();
            String oldValue = null;
            try {
                oldValue = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertNull(oldValue);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutIfAbsentAsync() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "putifabsentasync_" + i;
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            Future<String> future = ((MapProxyImpl<String, String>) map).putIfAbsentAsync(key, value).toCompletableFuture();
            String oldValue = null;
            try {
                oldValue = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertNull(oldValue);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testSetAsync() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "setasync_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            Future<Void> future = map.setAsync(key, value).toCompletableFuture();
            try {
                future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testEvict() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "evict_" + String.valueOf(i);
            String value = "merhaba-" + key;

            String oldValue = map.put(key, value);
            // this brings the value into the Near Cache
            String value1 = map.get(key);
            map.evict(key);
            // here we _might_ still see the value
            String value2 = map.get(key);

            assertNull(oldValue);
            assertEquals(value, value1);
            assertNull(value2);
        }
    }

    @Test
    public void testSet() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "set_" + String.valueOf(i);
            String value = "merhaba-" + key;

            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            map.set(key, value);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testReplace() {
        IMap<String, String> map = hz.getMap(getMapName());
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "replace_" + String.valueOf(i);
            String value = "merhaba-" + key;
            String valueNew = "merhaba-new" + key;

            map.put(key, value);
            // this brings the CACHED_AS_NULL into the Near Cache
            String value1 = map.get(key);
            String oldValue = map.replace(key, valueNew);
            // here we _might_ still see the CACHED_AS_NULL
            String value2 = map.get(key);

            assertNotNull(value1);
            assertEquals(value, oldValue);
            assertEquals(valueNew, value2);
        }
    }

    @Test
    public void testExecuteOnKey() {
        IMap<String, String> map = hz.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "executeOnKey_" + String.valueOf(i);
            // bring null local cache
            String expectedNull = map.get(key);
            assertNull(expectedNull);
            String newValue = (String) map.executeOnKey(key, new WritingEntryProcessor());
            String value = map.get(key);
            assertEquals(newValue, value);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteOnKeys() {
        IMap<String, String> map = hz.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            String key = "executeOnKeys_" + String.valueOf(i);
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
     * An entry processor which writes (changes the value and calls {@link Map.Entry#setValue(Object)}).
     */
    public static class WritingEntryProcessor implements EntryProcessor<String, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String process(Map.Entry<String, String> entry) {
            entry.setValue("new value");
            return "new value";
        }
    }
}
