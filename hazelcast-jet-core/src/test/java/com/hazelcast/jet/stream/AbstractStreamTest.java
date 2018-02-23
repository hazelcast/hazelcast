/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.ICacheJet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.TestInClusterSupport;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static org.junit.Assert.fail;

public abstract class AbstractStreamTest extends TestInClusterSupport {

    public static final int COUNT = 10000;

    protected DistributedStream<Entry<String, Integer>> streamMap() {
        IMapJet<String, Integer> map = getMap(testMode.getJet());
        fillMap(map);
        return DistributedStream.fromMap(map);
    }

    protected static DistributedStream<Map.Entry<String, Integer>> streamCache() {
        ICacheJet<String, Integer> cache = getCache();
        fillCache(cache);
        return cache.distributedStream();
    }

    protected DistributedStream<Integer> streamList() {
        IListJet<Integer> list = getList(testMode.getJet());
        fillList(list);
        return DistributedStream.fromList(list);
    }

    protected <K, V> IMapJet<K, V> getMap() {
        return getMap(testMode.getJet());
    }

    static <K, V> ICacheJet<K, V> getCache() {
        String cacheName = randomName();
        ICacheJet<K, V> cache = null;
        for (JetInstance jetInstance : allJetInstances()) {
            cache = jetInstance.getCacheManager().getCache(cacheName);
        }
        return cache;
    }

    protected JetInstance getInstance() {
        return testMode.getJet();
    }

    protected <E> IListJet<E> getList() {
        return getList(testMode.getJet());
    }

    static void fillMap(IMap<String, Integer> map) {
        fillMap(map, COUNT);
    }

    static void fillCache(ICache<String, Integer> cache) {
        fillCache(cache, COUNT);
    }

    private static void fillMap(IMap<String, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("key-" + i, i);
        }
    }

    private static void fillCache(ICache<String, Integer> cache, int count) {
        for (int i = 0; i < count; i++) {
            cache.put("key-" + i, i);
        }
    }

    static void fillList(IList<Integer> list) {
        fillListWithInts(list, COUNT);
    }

    static <R> void fillList(IList<R> list, Function<Integer, R> mapper) {
        for (int i = 0; i < COUNT; i++) {
            list.add(mapper.apply(i));
        }
    }

    static void fillList(IList<Integer> list, Iterator<Integer> iterator) {
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
    }

    static <T> List<T> sortedList(IList<T> list) {
        @SuppressWarnings("unchecked") T[] array = (T[]) new Object[list.size()];
        list.toArray(array);
        Arrays.sort(array);
        return Arrays.asList(array);
    }

    static <T extends Comparable<T>> void assertNotSorted(T[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[i - 1].compareTo(array[i]) > 0) {
                return;
            }
        }
        fail("Items in array " + Arrays.toString(array) + " were sorted.");
    }
}
