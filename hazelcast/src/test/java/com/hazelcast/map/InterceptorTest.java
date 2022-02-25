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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.StringUtil.LOCALE_INTERNAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InterceptorTest extends HazelcastTestSupport {

    @Test
    public void removeInterceptor_returns_true_when_interceptor_removed() {
        HazelcastInstance node = createHazelcastInstance();

        String mapName = "mapWithInterceptor";
        IMap map = node.getMap(mapName);
        String id = map.addInterceptor(new SimpleInterceptor());

        assertTrue(map.removeInterceptor(id));
        assertNoRegisteredInterceptorExists(map);
    }

    private static void assertNoRegisteredInterceptorExists(IMap map) {
        String mapName = map.getName();
        MapService mapservice = (MapService) (((MapProxyImpl) map).getService());
        mapservice.getMapServiceContext().getMapContainer(mapName).getInterceptorRegistry().getInterceptors();
    }

    @Test
    public void removeInterceptor_returns_false_when_there_is_no_interceptor() {
        HazelcastInstance node = createHazelcastInstance();

        IMap map = node.getMap("mapWithNoInterceptor");

        assertFalse(map.removeInterceptor(UuidUtil.newUnsecureUuidString()));
        assertNoRegisteredInterceptorExists(map);
    }

    @Test
    public void testMapInterceptor() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<Object, Object> map = hz.getMap("testMapInterceptor");
        String id = map.addInterceptor(new SimpleInterceptor());

        map.put(1, "New York");
        map.put(2, "Istanbul");
        map.put(3, "Tokyo");
        map.put(4, "London");
        map.put(5, "Paris");
        map.put(6, "Cairo");
        map.put(7, "Hong Kong");

        try {
            map.remove(1);
        } catch (Exception ignore) {
        }
        try {
            map.remove(2);
        } catch (Exception ignore) {
        }

        assertEquals(6, map.size());
        assertNull(map.get(1));
        assertEquals(map.get(2), "ISTANBUL:");
        assertEquals(map.get(3), "TOKYO:");
        assertEquals(map.get(4), "LONDON:");
        assertEquals(map.get(5), "PARIS:");
        assertEquals(map.get(6), "CAIRO:");
        assertEquals(map.get(7), "HONG KONG:");

        map.removeInterceptor(id);
        map.put(8, "Moscow");

        assertNull(map.get(1));
        assertEquals(map.get(2), "ISTANBUL");
        assertEquals(map.get(3), "TOKYO");
        assertEquals(map.get(4), "LONDON");
        assertEquals(map.get(5), "PARIS");
        assertEquals(map.get(6), "CAIRO");
        assertEquals(map.get(7), "HONG KONG");
        assertEquals(map.get(8), "Moscow");
    }

    @Test
    public void testMapInterceptorOnNewMember() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Object> map1 = hz1.getMap("map");

        for (int i = 0; i < 100; i++) {
            map1.put(i, i);
        }

        map1.addInterceptor(new NegativeGetInterceptor());
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, map1.get(i));
        }

        HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Object> map2 = hz2.getMap("map");
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, map1.get(i));
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, map2.get(i));
        }
    }

    @Test
    public void testGetAll_withGetInterceptor() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());

        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            map.put(i, String.valueOf(i));
            set.add(i);
        }

        Map<Integer, String> allValues = map.getAll(set);
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected intercepted value on map.getAll()", String.valueOf(i) + ":", allValues.get(i));
        }
    }

    @Test
    public void testPutEvent_withInterceptor() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());
        final EntryAddedLatch listener = new EntryAddedLatch();
        map.addEntryListener(listener, true);

        String value = "foo";
        map.put(1, value);

        final String expectedValue = value.toUpperCase(LOCALE_INTERNAL);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expectedValue, listener.getAddedValue());
            }
        }, 15);
    }

    @Test
    public void testPutEvent_withInterceptor_withEntryProcessor_withMultipleKeys() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());
        final EntryAddedLatch listener = new EntryAddedLatch();
        map.addEntryListener(listener, true);

        String value = "foo";
        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        map.executeOnKeys(keys, new EntryPutProcessor("foo"));

        final String expectedValue = value.toUpperCase(LOCALE_INTERNAL);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expectedValue, listener.getAddedValue());
            }
        }, 15);
    }

    @Test
    public void testPutEvent_withInterceptor_withEntryProcessor() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());
        final EntryAddedLatch listener = new EntryAddedLatch();
        map.addEntryListener(listener, true);

        String value = "foo";
        map.executeOnKey(1, new EntryPutProcessor("foo"));

        final String expectedValue = value.toUpperCase(LOCALE_INTERNAL);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expectedValue, listener.getAddedValue());
            }
        }, 15);
    }

    @Test
    public void testPutEvent_withInterceptor_withLoadAll() {
        String name = randomString();
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new DummyLoader());
        config.getMapConfig(name).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, String> map = instance.getMap(name);
        map.addInterceptor(new SimpleInterceptor());
        final EntryLoadedLatch listener = new EntryLoadedLatch();
        map.addEntryListener(listener, true);

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        map.loadAll(keys, false);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("FOO-1", listener.getLoadedValue());
            }
        }, 15);
    }

    @Test
    public void testInterceptPut_replicatedToBackups() {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = getConfig();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = hz2.getMap(name);
        map.addInterceptor(new NegativePutInterceptor());

        int count = 1000;
        for (int i = 1; i <= count; i++) {
            map.set(i, i);
        }
        waitAllForSafeState(hz1, hz2);

        hz1.getLifecycleService().terminate();

        for (int i = 1; i <= count; i++) {
            assertEquals(-i, map.get(i));
        }
    }

    @Test
    public void testInterceptPut_replicatedToBackups_usingTransactions() {
        final String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = getConfig();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = hz2.getMap(name);
        map.addInterceptor(new NegativePutInterceptor());

        final int count = 1000;
        hz2.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                TransactionalMap<Object, Object> txMap = context.getMap(name);
                for (int i = 1; i <= count; i++) {
                    txMap.set(i, i);
                }
                return null;
            }
        });
        waitAllForSafeState(hz1, hz2);

        hz1.getLifecycleService().terminate();

        for (int i = 1; i <= count; i++) {
            assertEquals(-i, map.get(i));
        }
    }

    static class DummyLoader implements MapLoader<Integer, String> {

        @Override
        public String load(Integer key) {
            return "foo-" + key;
        }

        @Override
        public Map<Integer, String> loadAll(Collection<Integer> keys) {
            Map<Integer, String> map = new HashMap<Integer, String>(keys.size());
            for (Integer key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return null;
        }
    }

    static class EntryPutProcessor implements EntryProcessor<Integer, String, String> {

        String value;

        EntryPutProcessor(String value) {
            this.value = value;
        }

        @Override
        public String process(Map.Entry<Integer, String> entry) {
            return entry.setValue(value);
        }
    }

    static class EntryAddedLatch implements EntryAddedListener<Integer, String> {

        AtomicReference<String> value = new AtomicReference<String>();

        @Override
        public void entryAdded(EntryEvent<Integer, String> event) {
            value.compareAndSet(null, event.getValue());
        }

        String getAddedValue() {
            return value.get();
        }
    }

    static class EntryLoadedLatch implements EntryLoadedListener<Integer, String> {

        AtomicReference<String> value = new AtomicReference<String>();

        @Override
        public void entryLoaded(EntryEvent<Integer, String> event) {
            value.compareAndSet(null, event.getValue());
        }

        String getLoadedValue() {
            return value.get();
        }
    }

    static class MapInterceptorAdaptor implements MapInterceptor {

        @Override
        public Object interceptGet(Object value) {
            return value;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return newValue;
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return removedValue;
        }

        @Override
        public void afterRemove(Object value) {
        }
    }

    public static class SimpleInterceptor extends MapInterceptorAdaptor {

        @Override
        public Object interceptGet(Object value) {
            if (value == null) {
                return null;
            }
            return value + ":";
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return newValue.toString().toUpperCase(LOCALE_INTERNAL);
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            if (removedValue.equals("ISTANBUL")) {
                throw new RuntimeException("you can not remove this");
            }
            return removedValue;
        }
    }

    static class NegativeGetInterceptor extends MapInterceptorAdaptor {

        @Override
        public Object interceptGet(Object value) {
            return ((Integer) value) * -1;
        }
    }

    static class NegativePutInterceptor extends MapInterceptorAdaptor {

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return ((Integer) newValue) * -1;
        }
    }
}
