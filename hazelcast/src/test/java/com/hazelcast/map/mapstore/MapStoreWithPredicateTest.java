package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.mapstore.MapStoreTest.newConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapStoreWithPredicateTest extends HazelcastTestSupport {

    @Test
    public void testKeySetWithPredicate_checksMapStoreLoad() throws InterruptedException {
        MapStoreTest.TestEventBasedMapStore testMapStore = new MapStoreTest.TestEventBasedMapStore();

        Map mapForStore = new HashMap();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Set expected = map.keySet(Predicates.greaterThan("value", 1));
                assertEquals(3, expected.size());
                assertTrue(expected.contains("key1"));
                assertTrue(expected.contains("key2"));
                assertTrue(expected.contains("key3"));
            }
        });
    }


    @Test
    public void testValuesWithPredicate_checksMapStoreLoad() throws InterruptedException {
        MapStoreTest.TestEventBasedMapStore testMapStore = new MapStoreTest.TestEventBasedMapStore();

        Map mapForStore = new HashMap();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final Collection values = map.values(Predicates.greaterThan("value", 1));
                assertEquals(3, values.size());
                assertTrue(values.contains(17));
                assertTrue(values.contains(37));
                assertTrue(values.contains(47));
            }
        });

    }

    @Test
    public void testEntrySetWithPredicate_checksMapStoreLoad() throws InterruptedException {
        MapStoreTest.TestEventBasedMapStore testMapStore = new MapStoreTest.TestEventBasedMapStore();

        Map mapForStore = new HashMap();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap("default");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final Set<Map.Entry> entrySet = map.entrySet(Predicates.greaterThan("value", 1));
                assertEquals(3, entrySet.size());
            }
        });

    }
}
