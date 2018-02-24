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
import com.hazelcast.jet.IListJet;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SortTest extends AbstractStreamTest {

    @Test
    public void sourceList() {
        IListJet<Integer> list = getList();
        fillList(list, IntStream.range(0, COUNT).map(i -> COUNT - i - 1).limit(COUNT).iterator());

        IList<Integer> result = DistributedStream.fromList(list)
                .sorted()
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(result);
    }

    @Test
    public void sourceMap() {
        IList<Integer> list = streamMap()
                .map(Entry::getValue)
                .sorted()
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    @Test
    public void sourceCache() {
        IList<Integer> list = streamCache()
                .map(Entry::getValue)
                .sorted()
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    @Test
    public void sourceMap_withComparator() {
        IList<Integer> list = streamMap()
                .map(Entry::getValue)
                .sorted((left, right) -> right.compareTo(left))
                .collect(DistributedCollectors.toIList(randomString()));

        assertListDescending(list);
    }

    @Test
    public void sourceCache_withComparator() {
        IList<Integer> list = streamCache()
                .map(Entry::getValue)
                .sorted((left, right) -> right.compareTo(left))
                .collect(DistributedCollectors.toIList(randomString()));

        assertListDescending(list);
    }

    @Test
    public void operationsAfterSort_sourceMap() {
        IList<Integer> list = streamMap()
                .map(Entry::getValue)
                .sorted(Integer::compareTo)
                .map(i -> i * i)
                .collect(DistributedCollectors.toIList(randomString()));

        assertListSquare(list);
    }

    @Test
    public void operationsAfterSort_sourceCache() {
        IList<Integer> list = streamCache()
                .map(Entry::getValue)
                .sorted(Integer::compareTo)
                .map(i -> i * i)
                .collect(DistributedCollectors.toIList(randomString()));

        assertListSquare(list);
    }

    private void assertList(IList<Integer> list) {
        assertEquals(COUNT, list.size());

        int i = 0;
        for (Integer val : list) {
            assertEquals(i++, (int) val);
        }
    }

    private void assertListDescending(IList<Integer> list) {
        assertEquals(COUNT, list.size());

        int i = COUNT - 1;
        for (Integer val : list) {
            assertEquals(i--, (int) val);
        }
    }

    private void assertListSquare(IList<Integer> list) {
        assertEquals(COUNT, list.size());
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, (int) list.get(i));
        }
    }


}
