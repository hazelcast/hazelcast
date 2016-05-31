/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.stream;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.fail;

public abstract class JetStreamTestSupport extends HazelcastTestSupport {

    public static final int COUNT = 10000;
    public static final int NODE_COUNT = 2;

    protected static HazelcastInstance instance;
    protected static TestHazelcastFactory hazelcastInstanceFactory;

    @BeforeClass
    public static void setupCluster() throws InterruptedException {
        setLogLevel(Level.INFO);
        hazelcastInstanceFactory = new TestHazelcastFactory();
        instance = hazelcastInstanceFactory.newHazelcastInstance();

        for (int i = 1; i < NODE_COUNT; i++) {
            hazelcastInstanceFactory.newHazelcastInstance();
        }
    }

    @AfterClass
    public static void shutdownCluster() {
        hazelcastInstanceFactory.shutdownAll();

    }

    protected static <K,V> IStreamMap<K,V> getMap(HazelcastInstance instance) {
        return IStreamMap.streamMap(instance, instance.getMap(randomName()));
    }

    protected static <E> IStreamList<E> getList(HazelcastInstance instance) {
        return IStreamList.streamList(instance, instance.getList(randomName()));
    }

    protected static int fillMap(IMap<String, Integer> map) {
        for (int i = 0; i < COUNT; i++) {
            map.put("key-" + i, i);
        }
        return COUNT;
    }

    protected static int fillMap(IMap<String, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("key-" + i, i);
        }
        return count;
    }

    protected static void fillList(IList<Integer> list) {
        for (int i = 0; i < COUNT; i++) {
            list.add(i);
        }
    }

    protected static <R> void fillList(IList<R> list, Function<Integer, R> mapper) {
        for (int i = 0; i < COUNT; i++) {
            list.add(mapper.apply(i));
        }
    }

    protected static void fillList(IList<Integer> list, Iterator<Integer> iterator) {
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
    }

    protected static <T> List<T> sortedList(IList<T> list) {
        T[] array = (T[])new Object[list.size()];
        list.toArray(array);
        Arrays.sort(array);
        return Arrays.asList(array);
    }

    protected <T extends Comparable<T>> void assertNotSorted(T[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[i-1].compareTo(array[i]) > 0) {
                return;
            }
        }
        fail("Items in array " + Arrays.toString(array) + " were sorted.");
    }
}
