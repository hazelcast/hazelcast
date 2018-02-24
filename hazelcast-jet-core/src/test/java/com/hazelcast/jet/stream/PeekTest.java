/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;

public class PeekTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        IMap<String, Integer> collected = streamMap()
                .peek(e -> runningTotal.addAndGet(e.getValue()))
                .collect(DistributedCollectors.toIMap(randomName()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
        assertEquals(COUNT, collected.size());
    }

    @Test
    public void sourceCache() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        IMap<String, Integer> collected = streamCache()
                .peek(e -> runningTotal.addAndGet(e.getValue()))
                .map(e -> entry(e.getKey(), e.getValue()))
                .collect(DistributedCollectors.toIMap(randomName()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
        assertEquals(COUNT, collected.size());
    }

    @Test
    public void sourceList() {
        final List<Integer> result = new ArrayList<>();
        IList<Integer> collected = streamList()
                .peek(result::add)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT, result.size());
        assertEquals(COUNT, collected.size());

        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) result.get(i));
        }
    }

    @Test
    public void intermediateOperation_sourceMap() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        IList<Integer> collected = streamMap()
                .map(Entry::getValue)
                .peek(runningTotal::addAndGet)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
        assertEquals(COUNT, collected.size());
    }

    @Test
    public void intermediateOperation_sourceCache() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        IList<Integer> collected = streamCache()
                .map(Entry::getValue)
                .peek(runningTotal::addAndGet)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
        assertEquals(COUNT, collected.size());
    }

}
