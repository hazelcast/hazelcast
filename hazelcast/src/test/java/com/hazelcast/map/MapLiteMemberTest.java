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
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLiteMemberTest
        extends HazelcastTestSupport {

    private Config liteConfig = new Config().setLiteMember(true);

    private TestHazelcastInstanceFactory factory;

    private IMap<Integer, Object> map;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(2);
        HazelcastInstance lite = factory.newHazelcastInstance(liteConfig);
        factory.newHazelcastInstance();
        map = lite.getMap(randomMapName());
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testMapPutOnLiteMember() {
        assertNull(map.put(1, 2));
    }

    @Test
    public void testMapGetOnLiteMember() {
        map.put(1, 2);
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMapSizeOnLiteMember() {
        map.put(1, 2);
        assertEquals(1, map.size());
    }

    @Test
    public void testMapLocalKeysOnLiteMember() {
        map.put(1, 1);

        final Set resultSet = map.localKeySet();
        assertNotNull(resultSet);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testMapEntryListenerOnLiteMember() {
        final DummyEntryListener listener = new DummyEntryListener();
        map.addEntryListener(listener, true);
        map.put(1, 2);

        assertTrueEventually(() -> {
            assertEquals(1, listener.key);
            assertEquals(2, listener.value);
        });
    }

    @Test
    public void testMapInterceptorOnLiteMember() {
        map.addInterceptor(new DummyMapInterceptor());
        map.put(1, "new");
        assertEquals("intercepted", map.get(1));
    }

    @Test
    public void testMapEntryProcessorOnLiteMember() {
        map.put(1, 2);

        final Map resultMap = this.map.executeOnEntries(new DummyEntryProcessor());
        assertEquals("dummy", map.get(1));
        assertEquals(1, resultMap.size());
        assertEquals("done", resultMap.get(1));
    }

    @Test
    public void testMapValuesQuery() {
        testMapValuesQuery(map);
    }

    @Test
    public void testMapKeysQuery() {
        testMapKeysQuery(map);
    }

    public static void testMapValuesQuery(final IMap<Integer, Object> map) {
        map.put(1, 2);

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        PredicateBuilder predicateBuilder = entryObject.key().equal(1);
        Collection values = map.values(predicateBuilder);

        assertEquals(1, values.size());
        assertEquals(2, values.iterator().next());
    }

    public static void testMapKeysQuery(final IMap<Integer, Object> map) {
        map.put(1, 2);

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        PredicateBuilder predicateBuilder = entryObject.key().equal(1);
        Collection values = map.keySet(predicateBuilder);

        assertEquals(1, values.size());
        assertEquals(1, values.iterator().next());
    }

    private static class DummyEntryListener implements EntryAddedListener<Object, Object> {

        private volatile Object key;

        private volatile Object value;

        @Override
        public void entryAdded(EntryEvent<Object, Object> event) {
            key = event.getKey();
            value = event.getValue();
        }
    }

    private static class DummyMapInterceptor implements MapInterceptor {

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            if (newValue.equals("new")) {
                return "intercepted";
            } else {
                throw new RuntimeException("no put");
            }
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {
        }
    }

    private static class DummyEntryProcessor implements EntryProcessor<Integer, Object, String> {

        @Override
        public String process(java.util.Map.Entry<Integer, Object> entry) {
            entry.setValue("dummy");
            return "done";
        }
    }
}
