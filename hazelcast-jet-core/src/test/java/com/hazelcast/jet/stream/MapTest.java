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

import java.util.Arrays;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static org.junit.Assert.assertEquals;

public class MapTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IList<Integer> list = map.stream()
                                 .map(e -> e.getValue() * e.getValue())
                                 .collect(DistributedCollectors.toIList(uniqueListName()));


        assertEquals(COUNT, list.size());

        Integer[] result = list.toArray(new Integer[COUNT]);
        Arrays.sort(result);
        for (int i = 0; i < COUNT; i++) {
            int val = result[i];
            assertEquals(i * i, val);
        }
    }

    @Test
    public void sourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        IList<Integer> result = list.stream()
                                    .map(i -> i * i)
                                    .collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(COUNT, result.size());

        for (int i = 0; i < COUNT; i++) {
            int val = result.get(i);
            assertEquals(i * i, val);
        }
    }
}
