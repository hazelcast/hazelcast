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
import com.hazelcast.map.impl.MapInterceptor;
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
public class InterceptorTest extends HazelcastTestSupport {

    @Test
    public void testMapInterceptor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        final IMap<Object, Object> map = instance1.getMap("testMapInterceptor");
        SimpleInterceptor interceptor = new SimpleInterceptor();
        String id = map.addInterceptor(interceptor);
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

        assertEquals(map.size(), 6);
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL:");
        assertEquals(map.get(3), "TOKYO:");
        assertEquals(map.get(4), "LONDON:");
        assertEquals(map.get(5), "PARIS:");
        assertEquals(map.get(6), "CAIRO:");
        assertEquals(map.get(7), "HONG KONG:");

        map.removeInterceptor(id);
        map.put(8, "Moscow");

        assertEquals(map.get(8), "Moscow");
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL");
        assertEquals(map.get(3), "TOKYO");
        assertEquals(map.get(4), "LONDON");
        assertEquals(map.get(5), "PARIS");
        assertEquals(map.get(6), "CAIRO");
        assertEquals(map.get(7), "HONG KONG");

    }

    static class SimpleInterceptor implements MapInterceptor, Serializable {

        @Override
        public Object interceptGet(Object value) {
            if (value == null)
                return null;
            return value + ":";
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
            if (removedValue.equals("ISTANBUL"))
                throw new RuntimeException("you can not remove this");
            return removedValue;
        }

        @Override
        public void afterRemove(Object value) {
        }
    }


    @Test
    public void testMapInterceptorOnNewMember() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        IMap map = instance1.getMap("map");
        for (int i = 0; i < 100; i++) {
            map.put(i,i);
        }
        map.addInterceptor(new NegativeInterceptor());
        for (int i = 0; i < 100; i++) {
            assertEquals(i*-1, map.get(i));
        }
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        for (int i = 0; i < 100; i++) {
            assertEquals(i*-1, map.get(i));
        }
    }

    static class NegativeInterceptor implements MapInterceptor, Serializable {
        @Override
        public Object interceptGet(Object value) {
            return ((Integer)value)*-1;
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
}
