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

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class MapTest extends JetStreamTestSupport {

    @Test
    public void testSimpleMap_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        IList<Integer> list = map.stream()
                .map(e -> e.getValue() * e.getValue())
                .collect(DistributedCollectors.toIList());


        assertEquals(COUNT, list.size());

        Integer[] result = list.toArray(new Integer[COUNT]);
        Arrays.sort(result);
        for (int i = 0; i < COUNT; i++) {
            int val = result[i];
            assertEquals(i * i, val);
        }
    }

    @Test
    public void testSimpleMap_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        IList<Integer> result = list.stream()
                .map(i -> i * i)
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT, result.size());

        for (int i = 0; i < COUNT; i++) {
            int val = result.get(i);
            assertEquals(i * i, val);
        }
    }
}
