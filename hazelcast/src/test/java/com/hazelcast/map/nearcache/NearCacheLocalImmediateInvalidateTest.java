package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Invalidates any map operation starter node's near cache immediately.
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NearCacheLocalImmediateInvalidateTest extends HazelcastTestSupport {

    private static final int numIterations = 1000;

    private static final long timeout = 100L;

    private static final int instanceCount = 2;

    private static final TimeUnit timeunit = TimeUnit.MILLISECONDS;

    private static final String mapName = NearCacheLocalImmediateInvalidateTest.class.getCanonicalName();

    private HazelcastInstance hcInstance;

    private HazelcastInstance hcInstance2;

    @Before
    public void setUp() throws Exception {
        // create config
        Config config = new Config();
        // configure near cache
        MapConfig mapConfig = config.getMapConfig(mapName + "*");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setEvictionPolicy("NONE");
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        // create Hazelcast instance
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        hcInstance = factory.newHazelcastInstance(config);
        hcInstance2 = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        hcInstance.getLifecycleService().shutdown();
        hcInstance2.getLifecycleService().shutdown();
    }

    @Test
    public void testRemove() {
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        for (int k = 0; k < numIterations; k++) {
            String key = "remove_" + String.valueOf(k);
            String value = "merhaba-" + key;
            // test
            String value0 = map.put(key, value);
            String value1 = map.get(key); // this brings the value into the NearCache
            String value2 = map.remove(key);
            String value3 = map.get(key); // here we _might_ still see the value

            assertNull(value0);
            assertEquals(value, value1);
            assertEquals(value, value2);
            assertNull(value3);

        }
    }

    @Test
    public void testDelete() {
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
            assertEquals(value, value1);
            assertNull(value3);
        }
    }

    @Test
    public void testRemoveValue() {
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        for (int k = 0; k < numIterations; k++) {
            String key = "evict_" + String.valueOf(k);
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        for (int k = 0; k < numIterations; k++) {
            String key = "set_" + String.valueOf(k);
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
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        for (int k = 0; k < numIterations; k++) {
            String key = "replace_" + String.valueOf(k);
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

    @Test
    public void testExecuteOnKey() {
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < numIterations; k++) {
            String key = "executeOnKey_" + String.valueOf(k);
            // bring null local cache.
            final String expectedNull = map.get(key);
            assertNull(expectedNull);
            final Object o = map.executeOnKey(key, new WritingEntryProcessor());
            final String newValue = (String) o;
            String value2 = map.get(key);
            assertEquals(newValue, value2);
        }
    }

    @Test
    public void testExecuteOnKeys() {
        final IMap<String, String> map = hcInstance.getMap(getMapName());
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < numIterations; k++) {
            String key = "executeOnKeys_" + String.valueOf(k);
            // bring null to local cache.
            final String expectedNull = map.get(key);
            assertNull(expectedNull);
            final HashSet<String> keys = new HashSet<String>();
            keys.add(key);
            final Object o = map.executeOnKeys(keys, new WritingEntryProcessor());
            final Map<String, String> result = (Map) o;
            for (Map.Entry<String, String> e : result.entrySet()) {
                final String newValue = e.getValue();
                final String cachedValue = map.get(e.getKey());
                assertEquals(newValue, cachedValue);
            }
        }
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

    private static String getMapName() {
        return randomMapName(mapName);
    }

}
