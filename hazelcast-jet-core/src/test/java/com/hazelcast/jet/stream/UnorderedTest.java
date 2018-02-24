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

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static org.junit.Assert.assertEquals;

public class UnorderedTest extends AbstractStreamTest {

    @Test
    public void orderedSource() {
        IList<Integer> list = streamList()
                .unordered()
                .map(i -> i)
                .collect(DistributedCollectors.toIList(uniqueListName()));

        assertNotSorted(list);
    }

    @Test
    public void orderedSource_asIntermediateOperation() {
        IList<Integer> list = streamList()
                .map(i -> i)
                .unordered()
                .map(i -> i)
                .collect(DistributedCollectors.toIList(uniqueListName()));

        assertNotSorted(list);
    }


    @Test
    public void unorderedSourceMap() {
        IList<Integer> list = streamMap()
                .unordered()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toIList(uniqueListName()));

        assertNotSorted(list);
    }

    @Test
    public void unorderedSourceCache() {
        IList<Integer> list = streamCache()
                .unordered()
                .map(Entry::getValue)
                .collect(DistributedCollectors.toIList(uniqueListName()));

        assertNotSorted(list);
    }

    private void assertNotSorted(IList<Integer> list) {
        assertEquals(COUNT, list.size());
        Integer[] result = list.toArray(new Integer[COUNT]);

        assertNotSorted(result);

        Arrays.sort(result);
        for (int i = 0; i < COUNT; i++) {
            assertEquals(i, (int) result[i]);
        }
    }


}
