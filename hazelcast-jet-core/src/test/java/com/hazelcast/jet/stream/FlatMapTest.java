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

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class FlatMapTest extends AbstractStreamTest {

    private static final int REPETITIONS = 10;

    @Test
    public void sourceMap() {
        IList<Integer> result = streamMap()
                .flatMap(e -> IntStream.iterate(e.getValue(), IntUnaryOperator.identity())
                                       .limit(REPETITIONS)
                                       .boxed())
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(result);
    }

    @Test
    public void sourceCache() {
        IList<Integer> result = streamCache()
                .flatMap(e -> IntStream.iterate(e.getValue(), IntUnaryOperator.identity())
                                       .limit(REPETITIONS)
                                       .boxed())
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(result);
    }

    @Test
    public void sourceList() {
        IList<Integer> result = streamList()
                .flatMap(i -> IntStream.iterate(i, IntUnaryOperator.identity())
                                       .limit(REPETITIONS)
                                       .boxed())
                .collect(DistributedCollectors.toIList(randomString()));

        assertList(result);
    }

    private void assertList(IList<Integer> result) {
        assertEquals(COUNT * REPETITIONS, result.size());

        List<Integer> sortedResult = sortedList(result);
        for (int i = 0; i < COUNT; i++) {
            for (int j = i; j < REPETITIONS; j++) {
                int val = sortedResult.get(i * REPETITIONS + j);
                assertEquals(i, val);
            }
        }
    }
}
