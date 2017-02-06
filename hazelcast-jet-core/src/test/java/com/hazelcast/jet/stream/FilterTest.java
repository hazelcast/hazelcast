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
import com.hazelcast.core.IMap;
import org.junit.Test;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueMapName;
import static org.junit.Assert.assertEquals;

public class FilterTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        IMap<String, Integer> result = map.stream()
                                          .filter(f -> f.getValue() < 10)
                                          .collect(DistributedCollectors.toIMap(uniqueMapName()));

        assertEquals(10, result.size());

        for (int i = 0; i < 10; i++) {
            int val = result.get("key-" + i);
            assertEquals(i, val);
        }
    }

    @Test
    public void sourceList() throws InterruptedException {
        IStreamList<Integer> list = getList();
        fillList(list);

        IList<Integer> result = list
                .stream()
                .filter(f -> f < 100)
                .collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(100, result.size());

        for (int i = 0; i < 100; i++) {
            int val = result.get(i);
            assertEquals(i, val);
        }
    }


}
