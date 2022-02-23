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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.StringUtil.LOCALE_INTERNAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EmbeddedMapInterceptorTest extends HazelcastTestSupport {

    private static final String[] CITIES = {"NEW YORK", "ISTANBUL", "TOKYO", "LONDON", "PARIS", "CAIRO", "HONG KONG"};

    private final String mapName = "testMapInterceptor";
    private final Map<HazelcastInstance, SimpleInterceptor> interceptorMap = new HashMap<HazelcastInstance, SimpleInterceptor>();

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;
    private HazelcastInstance hz2;
    private IMap<Object, Object> map1;
    private IMap<Object, Object> map2;
    private SimpleInterceptor interceptor1;
    private SimpleInterceptor interceptor2;

    private String key;
    private String value;
    private String expectedValueAfterPut;
    private String expectedValueAfterGet;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
        hz1 = startNodeWithInterceptor(factory);
        hz2 = startNodeWithInterceptor(factory);

        map1 = hz1.getMap(mapName);
        map2 = hz2.getMap(mapName);
        interceptor1 = interceptorMap.get(hz1);
        interceptor2 = interceptorMap.get(hz2);

        key = generateKeyOwnedBy(hz1);
        value = randomString();
        expectedValueAfterPut = value.toUpperCase(LOCALE_INTERNAL);
        expectedValueAfterGet = expectedValueAfterPut + "-foo";
    }

    /**
     * Test for issue #3931 (https://github.com/hazelcast/hazelcast/issues/3931)
     */
    @Test
    public void testChainingOfSameInterceptor() {
        putAll(map1, CITIES);

        assertGet(map1, "-foo", CITIES);
        assertGet(map2, "-foo", CITIES);
    }

    /**
     * Test for issue #3932 (https://github.com/hazelcast/hazelcast/issues/3932)
     */
    @Test
    public void testStoppingNodeLeavesInterceptor() {
        putAll(map1, CITIES);

        // terminate one node
        hz2.shutdown();

        assertGet(map1, "-foo", CITIES);

        // adding the node back in
        hz2 = startNodeWithInterceptor(factory);
        map2 = hz2.getMap(mapName);

        assertGet(map1, "-foo", CITIES);
        assertGet(map2, "-foo", CITIES);
    }

    @Test
    public void testGetInterceptedValue() {
        // no value is set yet
        assertNull("Expected no value", map1.get(key));
        assertNull("Expected no value", interceptor1.getValue);
        assertNull("Expected no value after get", interceptor1.afterGetValue);
        assertNoInteractionWith(interceptor2);
        interceptor1.reset();
        interceptor2.reset();

        map1.put(key, value);
        interceptor1.reset();
        interceptor2.reset();

        // multiple get calls
        for (int i = 0; i < 5; i++) {
            assertEquals("Expected the intercepted value", expectedValueAfterGet, map1.get(key));
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals("Expected the uppercase value", expectedValueAfterPut, interceptor1.getValue);
                    assertEquals("Expected the intercepted value after get", expectedValueAfterGet, interceptor1.afterGetValue);
                    assertNoInteractionWith(interceptor2);
                }
            });
            interceptor1.reset();
            interceptor2.reset();
        }
    }

    @Test
    public void testPutInterceptedValuePropagatesToBackupCorrectly() {
        map1.put(key, value);
        testInterceptedValuePropagatesToBackupCorrectly();
    }

    @Test
    public void testPutIfAbsentInterceptedValuePropagatesToBackupCorrectly() {
        map1.putIfAbsent(key, value);
        testInterceptedValuePropagatesToBackupCorrectly();
    }

    @Test
    public void testPutTransientInterceptedValuePropagatesToBackupCorrectly() {
        map1.putTransient(key, value, 1, TimeUnit.MINUTES);
        testInterceptedValuePropagatesToBackupCorrectly();
    }

    @Test
    public void testTryPutInterceptedValuePropagatesToBackupCorrectly() {
        map1.tryPut(key, value, 5, TimeUnit.SECONDS);
        testInterceptedValuePropagatesToBackupCorrectly();
    }

    @Test
    public void testSetInterceptedValuePropagatesToBackupCorrectly() {
        map1.set(key, value);
        testInterceptedValuePropagatesToBackupCorrectly();
    }

    private void testInterceptedValuePropagatesToBackupCorrectly() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull("Expected no old value", interceptor1.putOldValue);
                assertEquals("Expected unmodified new value", value, interceptor1.putNewValue);
                assertEquals("Expected new uppercase value after put", expectedValueAfterPut, interceptor1.afterPutValue);
                assertNoInteractionWith(interceptor2);
            }
        });
        assertEquals("Expected the intercepted value", expectedValueAfterGet, map1.get(key));

        hz1.getLifecycleService().shutdown();
        assertEquals("Expected the intercepted value after backup promotion", expectedValueAfterGet, map2.get(key));
    }

    @Test
    public void testReplaceInterceptedValuePropagatesToBackupCorrectly() {
        final String oldValue = "oldValue";
        final String oldValueAfterPut = oldValue.toUpperCase(LOCALE_INTERNAL);
        final String oldValueAfterGet = oldValueAfterPut + "-foo";

        map1.put(key, oldValue);
        assertEquals("Expected the intercepted old value", oldValueAfterGet, map1.get(key));
        interceptor1.reset();
        interceptor2.reset();

        map1.replace(key, value);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expected uppercase old value", oldValueAfterPut, interceptor1.putOldValue);
                assertEquals("Expected unmodified new value", value, interceptor1.putNewValue);
                assertEquals("Expected new uppercase value after put", expectedValueAfterPut, interceptor1.afterPutValue);
                assertNoInteractionWith(interceptor2);
            }
        });
        assertEquals("Expected the intercepted value", expectedValueAfterGet, map1.get(key));

        hz1.getLifecycleService().shutdown();
        assertEquals("Expected the intercepted value after backup promotion", expectedValueAfterGet, map2.get(key));
    }

    @Test
    public void testReplaceIfSameInterceptedValuePropagatesToBackupCorrectly() {
        final String oldValue = "oldValue";
        final String oldValueAfterPut = oldValue.toUpperCase(LOCALE_INTERNAL);
        final String oldValueAfterGet = oldValueAfterPut + "-foo";

        map1.put(key, oldValue);
        assertEquals("Expected the intercepted old value", oldValueAfterGet, map1.get(key));
        interceptor1.reset();
        interceptor2.reset();

        map1.replace(key, oldValueAfterPut, value);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expected uppercase old value", oldValueAfterPut, interceptor1.putOldValue);
                assertEquals("Expected unmodified new value", value, interceptor1.putNewValue);
                assertEquals("Expected new uppercase value after put", expectedValueAfterPut, interceptor1.afterPutValue);
                assertNoInteractionWith(interceptor2);
            }
        });
        assertEquals("Expected the intercepted value", expectedValueAfterGet, map1.get(key));

        hz1.getLifecycleService().shutdown();
        assertEquals("Expected the intercepted value after backup promotion", expectedValueAfterGet, map2.get(key));
    }

    @Test
    public void testRemoveInterceptedValuePropagatesToBackupCorrectly() {
        map1.put(key, value);
        assertEquals("Expected the intercepted value", expectedValueAfterGet, map1.get(key));
        interceptor1.reset();
        interceptor2.reset();

        map1.remove(key);
        assertEquals("Expected the uppercase removed value", expectedValueAfterPut, interceptor1.removedValue);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expected the uppercase value after remove", expectedValueAfterPut, interceptor1.afterRemoveValue);
            }
        });
        assertNoInteractionWith(interceptor2);
        assertNull("Expected the value to be removed", map1.get(key));

        hz1.getLifecycleService().shutdown();
        assertNull("Expected the value to be removed after backup promotion", map2.get(key));
    }

    /**
     * Starts a Hazelcast instance with a {@link SimpleInterceptor}.
     *
     * @param nodeFactory use this to create the instance
     * @return the started instance
     */
    private HazelcastInstance startNodeWithInterceptor(TestHazelcastInstanceFactory nodeFactory) {
        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(InMemoryFormat.OBJECT);
        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(mapName);

        SimpleInterceptor interceptor = new SimpleInterceptor();
        interceptorMap.put(instance, interceptor);
        map.addInterceptor(interceptor);

        return instance;
    }

    private static void putAll(IMap<Object, Object> map, String... cities) {
        for (int i = 1; i < cities.length; i++) {
            map.put(i, cities[i]);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertGet(IMap<Object, Object> map, String postfix, String... cities) {
        for (int i = 1; i < cities.length; i++) {
            assertEquals(cities[i] + postfix, map.get(i));
        }
    }

    private static void assertNoInteractionWith(SimpleInterceptor interceptor) {
        assertNull("Expected no getValue on the interceptor", interceptor.getValue);
        assertNull("Expected no afterGetValue on the interceptor", interceptor.afterGetValue);
        assertNull("Expected no putOldValue on the interceptor", interceptor.putOldValue);
        assertNull("Expected no putNewValue on the interceptor", interceptor.putNewValue);
        assertNull("Expected no afterPutValue on the interceptor", interceptor.afterPutValue);
        assertNull("Expected no removedValue on the interceptor", interceptor.removedValue);
        assertNull("Expected no afterRemoveValue on the interceptor", interceptor.afterRemoveValue);
    }

    static class SimpleInterceptor implements MapInterceptor, Serializable {

        volatile Object getValue;
        volatile Object afterGetValue;
        volatile Object putOldValue;
        volatile Object putNewValue;
        volatile Object afterPutValue;
        volatile Object removedValue;
        volatile Object afterRemoveValue;

        @Override
        public Object interceptGet(Object value) {
            getValue = value;
            if (value == null) {
                return null;
            }
            return value + "-foo";
        }

        @Override
        public void afterGet(Object value) {
            afterGetValue = value;
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            putOldValue = oldValue;
            putNewValue = newValue;
            return newValue.toString().toUpperCase(LOCALE_INTERNAL);
        }

        @Override
        public void afterPut(Object value) {
            afterPutValue = value;
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            this.removedValue = removedValue;
            return removedValue;
        }

        @Override
        public void afterRemove(Object oldValue) {
            afterRemoveValue = oldValue;
        }

        @Override
        public int hashCode() {
            return 123456;
        }

        void reset() {
            getValue = null;
            afterGetValue = null;
            putOldValue = null;
            putNewValue = null;
            afterPutValue = null;
            removedValue = null;
            afterRemoveValue = null;
        }
    }
}
