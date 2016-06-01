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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DistinctTest extends JetStreamTestSupport {

    @Test
    public void testDistinct_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        int modulus = 10;
        fillList(list, i -> i % modulus);

        IList<Integer> result = list
                .stream()
                .distinct()
                .collect(DistributedCollectors.toIList());

        assertEquals(modulus, result.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(Integer.toString(i), result.contains(i));
        }
    }

    @Test
    public void testDistinct_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int modulus = 10;
        IList<Integer> result = map
                .stream()
                .map(f -> f.getValue() % modulus)
                .distinct()
                .collect(DistributedCollectors.toIList());

        assertEquals(modulus, result.size());

        for (int i = 0; i < 10; i++) {
            assertTrue(Integer.toString(i), result.contains(i));
        }
    }


}
