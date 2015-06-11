/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EmbeddedMapInterceptorTest extends HazelcastTestSupport {

    private final String mapName = "testMapInterceptor";

    /**
     * Simulates initialisation code deployed on every member of the cluster. They
     * all add an interceptor because the same code is deployed everywhere.
     *
     * @param nodeFactory Use this to create the instance
     * @return The instance started
     */
    public HazelcastInstance startNode(TestHazelcastInstanceFactory nodeFactory) {
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap<Object, Object> map = instance.getMap(mapName);
        SimpleInterceptor interceptor = new SimpleInterceptor();
        map.addInterceptor(interceptor);
        return instance;
    }

    public void putAll(IMap<Integer, String> map, String... cities) {
        for (int i = 1; i < cities.length; i++) {
            map.put(i, cities[i]);
        }
    }

    public void assertGet(IMap<Integer, String> map, String postfix, String... cities) {
        for (int i = 1; i < cities.length; i++) {
            assertEquals(cities[i] + postfix, map.get(i));
        }
    }

    /**
     * Test for issue #3931 (https://github.com/hazelcast/hazelcast/issues/3931)
     *
     * @throws InterruptedException
     */
    @Test
    public void testChainingOfSameInterceptor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance i1 = startNode(nodeFactory);
        HazelcastInstance i2 = startNode(nodeFactory);

        final IMap<Integer, String> map1 = i1.getMap(mapName);
        final IMap<Integer, String> map2 = i2.getMap(mapName);

        String[] cities = {"NEW YORK", "ISTANBULL", "TOKYO", "LONDON", "PARIS", "CAIRO", "HONG KONG"};
        putAll(map1, cities);

        assertGet(map1, "-foo", cities);
        assertGet(map2, "-foo", cities);
    }

    /**
     * Test for issue #3932 (https://github.com/hazelcast/hazelcast/issues/3932)
     *
     * @throws InterruptedException
     */
    @Test
    public void testStoppingNodeLeavesInterceptor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance i1 = startNode(nodeFactory);
        HazelcastInstance i2 = startNode(nodeFactory);

        final IMap<Integer, String> map1 = i1.getMap(mapName);
        final IMap<Integer, String> map2 = i2.getMap(mapName);

        String[] cities = {"NEW YORK", "ISTANBULL", "TOKYO", "LONDON", "PARIS", "CAIRO", "HONG KONG"};
        putAll(map1, cities);

        //Now terminate one node
        i2.shutdown();

        assertGet(map1, "-foo", cities);

        //Now adding the node back in
        i2 = startNode(nodeFactory);
        IMap<Integer, String> map2b = i2.getMap(mapName);

        assertGet(map1, "-foo", cities);
        assertGet(map2b, "-foo", cities);
    }


    @Test
    public void testPutInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, key);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));

    }

    @Test
    public void testPutIfAbsentInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.putIfAbsent(key, key);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));

    }

    @Test
    public void testPutTransientInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.putTransient(key, key, 1, TimeUnit.MINUTES);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));

    }

    @Test
    public void testReplaceInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, key);
        map1.replace(key, key);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));

    }

    @Test
    public void testReplaceIfSameInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, key);
        map1.replace(key, key, key);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));
    }

    @Test
    public void testSetInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.set(key, key);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));
    }

    @Test
    public void testTryPutInterceptedValuePropagatesToBackupCorrectly() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = startNode(nodeFactory);
        HazelcastInstance h2 = startNode(nodeFactory);
        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.tryPut(key, key, 5, TimeUnit.SECONDS);
        assertEquals(key.toUpperCase() + "-foo", map1.get(key));
        h1.getLifecycleService().shutdown();
        assertEquals(key.toUpperCase() + "-foo", map2.get(key));
    }


    static class SimpleInterceptor implements MapInterceptor, Serializable {

        @Override
        public Object interceptGet(Object value) {
            if (value == null) {
                return null;
            }
            return value + "-foo";
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return newValue.toString().toUpperCase();
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

        @Override
        public int hashCode() {
            return 123456;
        }
    }

}
