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

import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SortTest extends JetStreamTestSupport {

    @Test
    public void testSort_whenSourceList() {
        IStreamList<Integer> list = getStreamList(instance);
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
    public void testSort_whenSourceMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(f -> f.getValue())
                .sorted()
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        int i = 0;
        for (Integer val : result) {
            assertEquals(i++, (int) val);
        }
    }

    @Test
    public void testSortWithComparator_whenSourceMap() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(f -> f.getValue())
                .sorted((left, right) -> right.compareTo(left))
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        int i = COUNT - 1;
        for (Integer val : result) {
            assertEquals(i--, (int) val);
        }
    }

    @Test
    public void testOperationsAfterSort() {
        IStreamMap<String, Integer> map = getStreamMap(instance);
        fillMap(map);

        IList<Integer> result = map
                .stream()
                .map(Map.Entry::getValue)
                .sorted((left, right) -> left.compareTo(right))
                .map(i -> i * i)
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i * i, (int)result.get(i));
        }
    }


}
