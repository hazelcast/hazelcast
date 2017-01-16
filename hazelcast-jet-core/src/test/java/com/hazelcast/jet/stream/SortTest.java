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

import com.hazelcast.core.IList;
import org.junit.Test;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SortTest extends AbstractStreamTest {

    @Test
    public void sourceList() {
        IStreamList<Integer> list = getList();
        fillList(list, IntStream.range(0, COUNT).map(i -> COUNT - i - 1).limit(COUNT).iterator());

        IList<Integer> result = list
                .stream()
                .sorted()
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        int i = 0;
        for (Integer val : result) {
            assertEquals(i++, (int) val);
        }
    }

    @Test
    public void sourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(Entry::getValue)
                .sorted()
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        int i = 0;
        for (Integer val : result) {
            assertEquals(i++, (int) val);
        }
    }

    @Test
    public void sourceMap_withComparator() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(Entry::getValue)
                .sorted((left, right) -> right.compareTo(left))
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        int i = COUNT - 1;
        for (Integer val : result) {
            assertEquals(i--, (int) val);
        }
    }

    @Test
    public void operationsAfterSort() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(Map.Entry::getValue)
                .sorted(Integer::compareTo)
                .map(i -> i * i)
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, (int) result.get(i));
        }
    }


}
