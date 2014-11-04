/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EmbeddedMapInterceptorTest extends HazelcastTestSupport {

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
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final IMap<Object, Object> map = instance1.getMap("testMapInterceptor");
        SimpleInterceptor interceptor = new SimpleInterceptor();
        String id = map.addInterceptor(interceptor);

        return instance1;
    }

    public void putAll(IMap<Integer, String> map, String... cities) {
        for (int i=1; i < cities.length; i++) {
            map.put(i, cities[i]);
        }
    }

    public void assertGet(IMap<Integer, String> map, String postfix, String... cities) {
        for (int i=1; i < cities.length; i++) {
            assertEquals(cities[i] + postfix, map.get(i));
        }
    }

    /**
     * Test for issue #3931 (https://github.com/hazelcast/hazelcast/issues/3931)
     * @throws InterruptedException
     */
    @Test
    public void testChainingOfSameInterceptor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance i1 = startNode(nodeFactory);
        HazelcastInstance i2 = startNode(nodeFactory);
        String MAPNAME = "testMapInterceptor";

        final IMap<Integer, String> map1 = i1.getMap(MAPNAME);
        final IMap<Integer, String> map2 = i2.getMap(MAPNAME);

        String[] cities = {"NEW YORK", "ISTANBULL", "TOKYO", "LONDON", "PARIS", "CAIRO", "HONG KONG"};
        putAll(map1, cities);

        assertGet(map1, "-foo", cities);
        assertGet(map2, "-foo", cities);
    }

    /**
     * Test for issue #3932 (https://github.com/hazelcast/hazelcast/issues/3932)
     * @throws InterruptedException
     */
    @Test
    public void testStoppingNodeLeavesInterceptor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance i1 = startNode(nodeFactory);
        HazelcastInstance i2 = startNode(nodeFactory);
        String MAPNAME = "testMapInterceptor";

        final IMap<Integer, String> map1 = i1.getMap(MAPNAME);
        final IMap<Integer, String> map2 = i2.getMap(MAPNAME);

        String[] cities = {"NEW YORK", "ISTANBULL", "TOKYO", "LONDON", "PARIS", "CAIRO", "HONG KONG"};
        putAll(map1, cities);

        //Now terminate one node
        i2.shutdown();

        //NOTE: I commented the following line because it makes the test fail. I want
        //      it to carry on a little more to illustrate the point.
        //      Once it is fixed delete these comments and uncomment the below line
        //assertGet(map1, "-foo", cities);

        //Now adding the node back in
        i2 = startNode(nodeFactory);
        IMap<Integer, String> map2b = i2.getMap(MAPNAME);

        assertGet(map1, "-foo", cities);
        assertGet(map2b, "-foo", cities);
    }

    static class SimpleInterceptor implements MapInterceptor, Serializable {

        @Override
        public Object interceptGet(Object value) {
            if (value == null)
                return null;
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
