package com.hazelcast.map;

import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NearCacheLocalInvalidationTest extends HazelcastTestSupport {

    private static final int numIterations = 1000;

    private static final long timeout = 100L;

    private static final TimeUnit timeunit = TimeUnit.MILLISECONDS;

    private static final String mapName = randomString();

    private HazelcastInstance hcInstance;

    @Before
    public void setUp() throws Exception {
        // create config
        Config config = new XmlConfigBuilder().build();
        // configure near cache
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setEvictionPolicy("NONE");
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        nearCacheConfig.setCacheLocalEntries(true); // this enables the local caching
        mapConfig.setNearCacheConfig(nearCacheConfig);
        // create Hazelcast instance
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        hcInstance = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        hcInstance.getLifecycleService().shutdown();
    }


    @Test
    public void testRemove() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "remove_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            String value2 = map.remove(key);
            String value3 = map.get(key); // here we _might_ still see the value

            assertNull(value0);
            assertEquals(value,value1);
            assertEquals(value, value2);
            assertNull(value3);

        }
    }

    @Test
    public void testDelete() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "delete_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            map.delete(key);
            String value3 = map.get(key); // here we _might_ still see the value
            // assert
            assertNull(value0);
            assertEquals(value,value1);
            assertNull(value3);
        }
    }

    @Test
    public void testRemoveValue() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "removevalue_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            map.remove(key, value);
            String value3 = map.get(key); // here we _might_ still see the value
            // assert
            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testTryRemove() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "tryremove_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            map.tryRemove(key, timeout, timeunit);
            String value3 = map.get(key); // here we _might_ still see the value
            // assert
            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testRemoveAsync() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "removeasync_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            Future<String> future = map.removeAsync(key);
            String value2 = null;
            try {
                value2 = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            String value3 = map.get(key); // here we _might_ still see the value
            // assert
            assertNull(value0);
            assertEquals(value, value1);
            assertEquals(value, value2);
            assertNull(value3);
        }
    }

    // -------------------------------------------------------------------- put

    @Test
    public void testPut() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "put_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            String value1 = map.put(key, value);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertNull(value1);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testTryPut() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "tryput_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            map.tryPut(key, value, timeout, timeunit);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutIfAbsent() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "putifabsent_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            String value1 = map.putIfAbsent(key, value);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertEquals(value, value2);
            assertNull(value1);
        }
    }

    @Test
    public void testPutTransient() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "puttransient_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            map.putTransient(key, value, 0, timeunit);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertEquals(value, value2);
        }
    }

    @Test
    public void testPutAsync() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "putasync_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            Future<String> future = map.putAsync(key, value);
            String value1 = null;
            try {
                value1 = future.get();
            } catch (Exception e) {
                fail("Exception in future.get(): " + e.getMessage());
            }
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertNull(value1);
            assertEquals(value, value2);
        }
    }


    @Test
    public void testEvict() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "remove_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            map.evict(key);
            String value3 = map.get(key); // here we _might_ still see the value

            assertNull(value0);
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testSet() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "put_" + String.valueOf(k);
            String value = "merhaba-" + key;

            // test
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            map.set(key, value);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNull(value0);
            assertEquals(value, value2);


        }
    }

    @Test
    public void testReplace() {
        final IMap<String, String> map = hcInstance.getMap(mapName);
        for (int k = 0; k < numIterations; k++) {
            String key = "put_" + String.valueOf(k);
            String value = "merhaba-" + key;
            String valueNew = "merhaba-new" + key;

            // test
            map.put(key, value);
            String value0 = map.get(key); // this brings the NULL_OBJECT into the NearCache
            map.replace(key, valueNew);
            String value2 = map.get(key); // here we _might_ still see the NULL_OBJECT
            // assert
            assertNotNull(value0);
            assertEquals(valueNew, value2);
        }
    }

}
