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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetTestSupport;
import org.apache.log4j.Level;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.fail;

public abstract class JetStreamTestSupport extends JetTestSupport {

    public static final int COUNT = 10000;
    public static final int NODE_COUNT = 4;

    protected static HazelcastInstance instance;

    @BeforeClass
    public static void setupCluster() throws InterruptedException {
        setLogLevel(Level.INFO);
        instance = createCluster(NODE_COUNT);
    }

    protected static <K, V> IStreamMap<K, V> getStreamMap(HazelcastInstance instance) {
        return IStreamMap.streamMap(getMap(instance));
    }

    protected static <E> IStreamList<E> getStreamList(HazelcastInstance instance) {
        return IStreamList.streamList(getList(instance));
    }

    protected static int fillMap(IMap<String, Integer> map) {
        return fillMap(map, COUNT);
    }

    protected static int fillMap(IMap<String, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("key-" + i, i);
        }
        return count;
    }

    protected static void fillList(IList<Integer> list) {
        fillListWithInts(list, COUNT);
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
        @SuppressWarnings("unchecked") T[] array = (T[]) new Object[list.size()];
        list.toArray(array);
        Arrays.sort(array);
        return Arrays.asList(array);
    }

    protected <T extends Comparable<T>> void assertNotSorted(T[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[i - 1].compareTo(array[i]) > 0) {
                return;
            }
        }
        fail("Items in array " + Arrays.toString(array) + " were sorted.");
    }
}
