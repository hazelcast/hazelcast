package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.InMemoryFormat;
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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NearCacheLocalInvalidationTest extends HazelcastTestSupport {

    private static final int numIterations = 1000;

    private static final long timeout = 100L;

    private static final TimeUnit timeunit = TimeUnit.MILLISECONDS;

    private static final String mapName = NearCacheLocalInvalidationTest.class.getCanonicalName();

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
            assertEquals(value, value1);
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
            assertEquals(value, value1);
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
        final IMap<String, String> map = hcInstance.getMap(mapName);
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
        final IMap<String, String> map = hcInstance.getMap(mapName);
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
        final IMap<String, Text> map = hcInstance.getMap(mapName);
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < numIterations; k++) {
            String key = "executeOnKey_" + String.valueOf(k);
            Text value0 = new Text("content" + k);
            // put and get to make sure entry exists *and* is in local near cache
            map.put(key, value0);
            map.get(key);
            Text value1 = map.get(key);
            Text value2 = map.get(key);
            assertTrue("reference in local near cache must be the same (k=" + k + ")" + value1 + " =? " + value2, value1 == value2);
            // execute reading entry processor (no invalidation required)
            final HashSet<String> strings = new HashSet<String>();
            strings.add(key);
            map.executeOnKey(key, new ReadingEntryProcessor());
            // execute writing entry processor (this must locally invalidate near cache)
            map.executeOnKey(key, new WritingEntryProcessor());
            Text value4 = map.get(key);
            assertFalse("reference must change by writing (k=" + k + ")", value1 == value4);
        }
    }

    @Test
    public void testExecuteOnKeys() {
        final IMap<String, Text> map = hcInstance.getMap(mapName);
        // loop over several keys to make sure we have keys on both instances
        for (int k = 0; k < numIterations; k++) {
            String key = "executeOnKeys_" + String.valueOf(k);
            Text value0 = new Text("content" + k);
            // put and get to make sure entry exists *and* is in local near cache
            map.put(key, value0);
            map.get(key);
            Text value1 = map.get(key);
            Text value2 = map.get(key);
            assertTrue("reference in local near cache must be the same (k=" + k + ")" + value1 + " =? " + value2,
                    value1 == value2);
            // execute reading entry processor (no invalidation required)
            final HashSet<String> keys = new HashSet<String>();
            keys.add(key);
            map.executeOnKeys(keys, new ReadingEntryProcessor());
            // execute writing entry processor (this must locally invalidate near cache)
            map.executeOnKeys(keys, new WritingEntryProcessor());
//            map.executeOnKeys(strings, new WritingEntryProcessor());
            Text value4 = map.get(key);
            assertFalse("reference must change by writing (k=" + k + ")", value1 == value4);
        }
    }

    /**
     * An entry processor which only reads.
     */
    public static class ReadingEntryProcessor extends AbstractEntryProcessor<String, Text> {

        private static final long serialVersionUID = 1L;

        @Override
        public Object process(Map.Entry<String, Text> entry) {
            return "read";
        }

    }

    /**
     * An entry processor which writes (changes the value and calls Entry.setValue()).
     */
    public static class WritingEntryProcessor extends AbstractEntryProcessor<String, Text> {

        private static final long serialVersionUID = 1L;

        @Override
        public Object process(Map.Entry<String, Text> entry) {
            Text text = entry.getValue();
            text.setContent("new content");
            entry.setValue(text);
            return "written";
        }
    }

    public static class Text implements Serializable {

        private static final long serialVersionUID = 1L;

        private String content = "original content";

        public Text(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        @Override
        public String toString() {
            return getContent();
        }
    }

}
