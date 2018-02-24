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
import org.junit.Test;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class MultipleTransformsTest extends AbstractStreamTest {

    private static final int COUNT = 10;

    @Test
    public void sourceMap() {
        IList<Integer> list = streamMap()
                .map(Entry::getValue)
                .filter(e -> e < COUNT)
                .flatMap(Stream::of)
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    @Test
    public void sourceCache() {
        IList<Integer> list = streamCache()
                .map(Entry::getValue)
                .filter(e -> e < COUNT)
                .flatMap(Stream::of)
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    @Test
    public void sourceList() {
        IList<Integer> list = streamList()
                .filter(e -> e < COUNT)
                .map(e -> e * e)
                .flatMap(Stream::of)
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list, true);
    }

    @Test
    public void intermediateOperation_sourceMap() {
        IList<Integer> list = streamMap()
                .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue()))
                .map(Entry::getValue)
                .filter(e -> e < COUNT)
                .flatMap(Stream::of)
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    @Test
    public void intermediateOperation_sourceCache() {
        IList<Integer> list = streamCache()
                .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue()))
                .map(Entry::getValue)
                .filter(e -> e < COUNT)
                .flatMap(Stream::of)
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(list);
    }

    private void assertList(IList<Integer> list) {
        assertList(list, false);
    }

    private void assertList(IList<Integer> list, boolean square) {
        assertEquals(COUNT, list.size());
        Integer[] result = list.toArray(new Integer[COUNT]);
        Arrays.sort(result);
        for (int i = 0; i < COUNT; i++) {
            int val = result[i];
            assertEquals(i * (square ? i : 1), val);
        }
    }
}
