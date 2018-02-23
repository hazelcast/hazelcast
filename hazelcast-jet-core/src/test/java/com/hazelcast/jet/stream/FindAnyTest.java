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


import com.hazelcast.jet.ICacheJet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FindAnyTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IMapJet<String, Integer> map = getMap();
        fillMap(map);

        Optional<Entry<String, Integer>> first = DistributedStream.fromMap(map).findAny();

        assertTrue(first.isPresent());
        Entry<String, Integer> entry = first.get();

        assertTrue(map.containsKey(entry.getKey()));
        assertEquals(map.get(entry.getKey()), entry.getValue());
    }

    @Test
    public void sourceCache() {
        ICacheJet<String, Integer> cache = getCache();
        fillCache(cache);

        Optional<Entry<String, Integer>> first = cache.distributedStream().findAny();

        assertTrue(first.isPresent());
        Entry<String, Integer> entry = first.get();

        assertTrue(cache.containsKey(entry.getKey()));
        assertEquals(cache.get(entry.getKey()), entry.getValue());
    }

    @Test
    public void sourceEmptyMap() {
        IMapJet<String, Integer> map = getMap();

        Optional<Entry<String, Integer>> first = DistributedStream.fromMap(map).findAny();

        assertFalse(first.isPresent());
    }

    @Test
    public void sourceEmptyCache() {
        ICacheJet<String, Integer> cache = getCache();

        Optional<Entry<String, Integer>> first = cache.distributedStream().findAny();

        assertFalse(first.isPresent());
    }

    @Test
    public void sourceList() {
        IListJet<Integer> list = getList();
        fillList(list);

        Optional<Integer> first = DistributedStream.fromList(list).findFirst();

        assertTrue(first.isPresent());
        assertTrue(list.contains(first.get()));
    }

    @Test
    public void sourceEmptyList() {
        IListJet<Integer> list = getList();

        Optional<Integer> first = DistributedStream.fromList(list).findAny();

        assertFalse(first.isPresent());
    }
}
