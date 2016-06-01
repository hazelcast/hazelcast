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
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TransformTest extends JetStreamTestSupport {

    @Test
    public void testMultipleTransforms_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int count = 10;
        IList<Integer> list = map.stream()
                .map(e -> e.getValue())
                .filter(e -> e <  count)
                .flatMap(e -> Stream.of(e))
                .collect(DistributedCollectors.toIList());

        assertEquals(count, list.size());
        Integer[] result = list.toArray(new Integer[count]);
        Arrays.sort(result);
        for (int i = 0; i < count; i++) {
            int val = result[i];
            assertEquals(i, val);
        }
    }

    @Test
    public void testMultipleTransforms_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        int count = 10;
        IList<Integer> result = list.stream()
                .filter(e -> e < count)
                .map(e -> e * e)
                .flatMap(e -> Stream.of(e))
                .collect(DistributedCollectors.toIList());

        assertEquals(count, result.size());

        for (int i = 0; i < count; i++) {
            int val = result.get(i);
            assertEquals(i * i, val);
        }
    }

    @Test
    public void testMultipleTransform_whenIntermediateOperation() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int count = 10;
        IList<Integer> list = map.stream()
                .sorted((Distributed.Comparator<Map.Entry<String, Integer>>) (o1, o2) -> o1.getValue().compareTo(o2.getValue()))
                .map(e -> e.getValue())
                .filter(e -> e <  count)
                .flatMap(e -> Stream.of(e))
                .collect(DistributedCollectors.toIList());

        assertEquals(count, list.size());
        Integer[] result = list.toArray(new Integer[count]);
        for (int i = 0; i < count; i++) {
            int val = result[i];
            assertEquals(i, val);
        }
    }
}
