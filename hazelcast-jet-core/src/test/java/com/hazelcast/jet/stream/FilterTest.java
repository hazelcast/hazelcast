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
import com.hazelcast.jet.IListJet;
import org.junit.Test;

import java.util.AbstractMap;

import static org.junit.Assert.assertEquals;

public class FilterTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IMap<String, Integer> result = streamMap()
                .filter(f -> f.getValue() < 10)
                .collect(DistributedCollectors.toIMap(randomName()));

        assertEquals(10, result.size());

        for (int i = 0; i < 10; i++) {
            int val = result.get("key-" + i);
            assertEquals(i, val);
        }
    }

    @Test
    public void sourceCache() {
        ICache<String, Integer> result = streamCache()
                .filter(f -> f.getValue() < 10)
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()))
                .collect(DistributedCollectors.toICache(randomName()));

        assertEquals(10, result.size());

        for (int i = 0; i < 10; i++) {
            int val = result.get("key-" + i);
            assertEquals(i, val);
        }
    }

    @Test
    public void sourceList() {
        IListJet<Integer> list = getList();
        fillList(list);

        IList<Integer> result = DistributedStream
                .fromList(list)
                .filter(f -> f < 100)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(100, result.size());

        for (int i = 0; i < 100; i++) {
            int val = result.get(i);
            assertEquals(i, val);
        }
    }


}
