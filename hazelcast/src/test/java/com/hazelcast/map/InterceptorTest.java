/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import java.io.Serial;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.timeout;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InterceptorTest extends HazelcastTestSupport {

    @Parameters(name = "inMemoryFormat:{0}")
    public static Object[] memoryFormat() {
        return new Object[]{
                InMemoryFormat.BINARY,
                InMemoryFormat.OBJECT,
        };
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    protected void put(IMap map, Object key, Object value) {
        map.put(key, value);
    }

    protected Object get(IMap map, Object key) {
        return map.get(key);
    }

    protected Object remove(IMap map, Object key) {
        return map.remove(key);
    }

    protected Map getAll(IMap map, Set keys) {
        return map.getAll(keys);
    }

    @Override
    protected Config getConfig() {
        var config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig("default")
                .setInMemoryFormat(inMemoryFormat);
        return config;
    }

    @Test
    public void removeInterceptor_returns_true_when_interceptor_removed() {
        HazelcastInstance node = createHazelcastInstance(getConfig());

        String mapName = "mapWithInterceptor";
        IMap<Object, Object> map = node.getMap(mapName);
        String id = map.addInterceptor(new SimpleInterceptor());

        assertTrue(map.removeInterceptor(id));
        assertNoRegisteredInterceptorExists(map);
    }

    private static void assertNoRegisteredInterceptorExists(IMap<Object, Object> map) {
        String mapName = map.getName();
        MapService mapservice = (MapService) (((MapProxyImpl) map).getService());
        mapservice.getMapServiceContext().getMapContainer(mapName).getInterceptorRegistry().getInterceptors();
    }

    @Test
    public void removeInterceptor_returns_false_when_there_is_no_interceptor() {
        HazelcastInstance node = createHazelcastInstance(getConfig());

        IMap<Object, Object> map = node.getMap("mapWithNoInterceptor");

        assertFalse(map.removeInterceptor(UuidUtil.newUnsecureUuidString()));
        assertNoRegisteredInterceptorExists(map);
    }

    @Test
    public void testMapInterceptor() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = nodeFactory.newHazelcastInstance(getConfig());
        nodeFactory.newHazelcastInstance(getConfig());

        IMap<Object, Object> map = hz.getMap("testMapInterceptor");
        String id = map.addInterceptor(new SimpleInterceptor());

        put(map, 1, "New York");
        put(map, 2, "Istanbul");
        put(map, 3, "Tokyo");
        put(map, 4, "London");
        put(map, 5, "Paris");
        put(map, 6, "Cairo");
        put(map, 7, "Hong Kong");

        try {
            remove(map, 1);
        } catch (Exception ignore) {
        }
        try {
            remove(map, 2);
        } catch (Exception ignore) {
        }

        assertEquals(6, map.size());
        assertNull(get(map, 1));
        assertEquals("ISTANBUL:", get(map, 2));
        assertEquals("TOKYO:", get(map, 3));
        assertEquals("LONDON:", get(map, 4));
        assertEquals("PARIS:", get(map, 5));
        assertEquals("CAIRO:", get(map, 6));
        assertEquals("HONG KONG:", get(map, 7));

        map.removeInterceptor(id);
        put(map, 8, "Moscow");

        assertNull(get(map, 1));
        assertEquals("ISTANBUL", get(map, 2));
        assertEquals("TOKYO", get(map, 3));
        assertEquals("LONDON", get(map, 4));
        assertEquals("PARIS", get(map, 5));
        assertEquals("CAIRO", get(map, 6));
        assertEquals("HONG KONG", get(map, 7));
        assertEquals("Moscow", get(map, 8));
    }

    @Test
    public void testMapInterceptorOnNewMember() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = nodeFactory.newHazelcastInstance(getConfig());
        IMap<Integer, Object> map1 = hz1.getMap("map");

        for (int i = 0; i < 100; i++) {
            put(map1, i, i);
        }

        map1.addInterceptor(new NegativeGetInterceptor());
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, get(map1, i));
        }

        HazelcastInstance hz2 = nodeFactory.newHazelcastInstance(getConfig());
        IMap<Integer, Object> map2 = hz2.getMap("map");
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, get(map1, i));
            assertEquals("Expected negative value on map1.get(" + i + ")", i * -1, get(map2, i));
        }
    }

    @Test
    public void testGetAll_withGetInterceptor() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());

        Set<Integer> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            put(map, i, String.valueOf(i));
            set.add(i);
        }

        Map<Integer, String> allValues = getAll(map, set);
        for (int i = 0; i < 100; i++) {
            assertEquals("Expected intercepted value on map.getAll()", i + ":", allValues.get(i));
        }
    }

    @Test
    public void testReadFromBackup_withGetInterceptor() {
        Config config = getConfig();
        config.getMapConfig("default").setReadBackupData(true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config, 2);
        IMap<String, Integer> node1Map = instances[0].getMap(randomString());
        node1Map.addInterceptor(new NegativeGetInterceptor());

        Map<String, Integer> keysOwnedByNode2 = new HashMap<>(100);
        for (int i = 1; i <= 100; i++) {
            String key = randomNameOwnedBy(instances[1]);
            put(node1Map, key, i);
            keysOwnedByNode2.put(key, i);
        }

        for (Map.Entry<String, Integer> entry : keysOwnedByNode2.entrySet()) {
            Integer value = node1Map.get(entry.getKey());
            assertEquals("Expected negative value on map.get(" + entry.getKey() + ")", -entry.getValue(), value.intValue());
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
        put(map, 1, value);

        final String expectedValue = StringUtil.upperCaseInternal(value);
        assertTrueEventually(() -> assertEquals(expectedValue, listener.getAddedValue()), 15);
    }

    @Test
    public void testPutEvent_withInterceptor_withEntryProcessor_withMultipleKeys() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomString());
        map.addInterceptor(new SimpleInterceptor());
        final EntryAddedLatch listener = new EntryAddedLatch();
        map.addEntryListener(listener, true);

        String value = "foo";
        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        map.executeOnKeys(keys, new EntryPutProcessor("foo"));

        final String expectedValue = StringUtil.upperCaseInternal(value);
        assertTrueEventually(() -> assertEquals(expectedValue, listener.getAddedValue()), 15);
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

        final String expectedValue = StringUtil.upperCaseInternal(value);
        assertTrueEventually(() -> assertEquals(expectedValue, listener.getAddedValue()), 15);
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

        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        map.loadAll(keys, false);

        assertTrueEventually(() -> assertEquals("FOO-1", listener.getLoadedValue()), 15);
    }

    @Test
    public void testInterceptPut_replicatedToBackups() {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance hz2 = factory.newHazelcastInstance(getConfig());

        IMap<Object, Object> map = hz2.getMap(name);
        map.addInterceptor(new NegativePutInterceptor());

        int count = 1000;
        for (int i = 1; i <= count; i++) {
            put(map, i, i);
        }
        waitAllForSafeState(hz1, hz2);

        hz1.getLifecycleService().terminate();

        for (int i = 1; i <= count; i++) {
            assertEquals(-i, get(map, i));
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
        hz2.executeTransaction(context -> {
            TransactionalMap<Object, Object> txMap = context.getMap(name);
            for (int i = 1; i <= count; i++) {
                txMap.set(i, i);
            }
            return null;
        });
        waitAllForSafeState(hz1, hz2);

        hz1.getLifecycleService().terminate();

        for (int i = 1; i <= count; i++) {
            assertEquals(-i, get(map, i));
        }
    }

    // The interceptor sets the new value based on the result from the previous interceptor.
    // The old value used by the interceptor is the original one before any changes.
    @Test
    public void chainOfPutInterceptor_useNewValueFromPreviousInterceptorAndOriginOldValue() {
        final String name = "chain-interceptors-test";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = hz.getMap(name);
        put(map, 1, 1);
        assertEquals(1, get(map, 1));

        map.addInterceptor(new NegativePutInterceptor());
        var secondInterceptor = Mockito.mock(MapInterceptor.class);
        map.addInterceptor(secondInterceptor);

        put(map, 1, 2);
        Mockito.verify(secondInterceptor).interceptPut(1, -2);
        Mockito.verify(secondInterceptor, timeout(ASSERT_TRUE_EVENTUALLY_TIMEOUT)).afterPut(-2);
    }

    @Test
    public void testAfterGetModifyInputValue_noAffectToStoredValue() {
        final String name = "input-value-modify-interceptor-test";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Dummy> map = hz.getMap(name);
        var value = new Dummy(1);
        put(map, 1, new Dummy(1));

        var id = map.addInterceptor(new AfterGetModifyInterceptor());

        assertEquals(value, get(map, 1));
        map.removeInterceptor(id);
        assertEquals(value, get(map, 1));
    }

    static class DummyLoader implements MapLoader<Integer, String> {

        @Override
        public String load(Integer key) {
            return "foo-" + key;
        }

        @Override
        public Map<Integer, String> loadAll(Collection<Integer> keys) {
            Map<Integer, String> map = new HashMap<>(keys.size());
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

        AtomicReference<String> value = new AtomicReference<>();

        @Override
        public void entryAdded(EntryEvent<Integer, String> event) {
            value.compareAndSet(null, event.getValue());
        }

        String getAddedValue() {
            return value.get();
        }
    }

    static class EntryLoadedLatch implements EntryLoadedListener<Integer, String> {

        AtomicReference<String> value = new AtomicReference<>();

        @Override
        public void entryLoaded(EntryEvent<Integer, String> event) {
            value.compareAndSet(null, event.getValue());
        }

        String getLoadedValue() {
            return value.get();
        }
    }

    public static class SimpleInterceptor extends MapInterceptorAdaptor {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public Object interceptGet(Object value) {
            if (value == null) {
                return null;
            }
            return value + ":";
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return StringUtil.upperCaseInternal(newValue.toString());
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
            return value == null ? null : ((Integer) value) * -1;
        }
    }

    static class NegativePutInterceptor extends MapInterceptorAdaptor {

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return ((Integer) newValue) * -1;
        }
    }

    static class AfterGetModifyInterceptor extends MapInterceptorAdaptor {
        @Override
        public void afterGet(Object value) {
            if (value instanceof Dummy) {
                Dummy dummy = (Dummy) value;
                dummy.value = -dummy.value;
            }
        }
    }

    public static class Dummy {
        int value;

        public Dummy(int value) {
            this.value = value;
        }

        public Dummy() {
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Dummy dummy = (Dummy) object;
            return value == dummy.value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }

        @Override
        public String toString() {
            return "Dummy{" + "value=" + value + '}';
        }
    }
}
