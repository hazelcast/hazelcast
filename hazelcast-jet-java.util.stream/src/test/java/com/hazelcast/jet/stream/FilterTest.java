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
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FilterTest extends JetStreamTestSupport {

    @Test
    public void testSimpleFilter_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        IMap<String, Integer> result = map.stream()
                .filter(f -> f.getValue() < 10)
                .collect(DistributedCollectors.toIMap());

        assertEquals(10, result.size());

        for (int i = 0; i < 10; i++) {
            int val = result.get("key-" + i);
            assertEquals(i, val);
        }
    }

    @Test
    public void testSimpleFilter_whenSourceList() throws InterruptedException {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        IList<Integer> result = list
                .stream()
                .filter(f -> f < 100)
                .collect(DistributedCollectors.toIList());

        assertEquals(100, result.size());

        for (int i = 0; i < 100; i++) {
            int val = result.get(i);
            assertEquals(i, val);
        }
    }


}
