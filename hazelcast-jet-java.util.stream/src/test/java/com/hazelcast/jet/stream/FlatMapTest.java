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

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FlatMapTest extends JetStreamTestSupport {

    @Test
    public void testFlatMap_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap(instance);
        fillMap(map);

        int repetitions = 10;

        IList<Integer> result = map.stream()
                .flatMap(e -> IntStream.iterate(e.getValue(), IntUnaryOperator.identity())
                        .limit(repetitions)
                        .boxed())
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT * repetitions, result.size());

        List<Integer> sortedResult = sortedList(result);
        for (int i = 0; i < COUNT; i++) {
            for (int j = i; j < repetitions; j++) {
                int val = sortedResult.get(i * repetitions + j);
                assertEquals(i, val);
            }
        }
    }

    @Test
    public void testFlatMap_whenSourceList() {
        IStreamList<Integer> list = getList(instance);
        fillList(list);

        int repetitions = 10;

        IList<Integer> result = list.stream()
                .flatMap(i -> IntStream.iterate(i, IntUnaryOperator.identity())
                                .limit(repetitions)
                                .boxed())
                .collect(DistributedCollectors.toIList());

        assertEquals(COUNT * repetitions, result.size());

        for (int i = 0; i < COUNT; i++) {
            for (int j = i; j < repetitions; j++) {
                int val = result.get(i * repetitions + j);
                assertEquals(i, val);
            }
        }
    }
}
