/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.groupingBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.toAggregator;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateOperationToAggregatorTest extends PipelineTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_missingCombineFn() {
        AggregateOperation1<Object, Void, Void> aggrOp =
            AggregateOperation.<Void>withCreate(() -> null)
            .andAccumulate((v, t) -> { })
            .andExportFinish(v -> v);

        expectedException.expect(NullPointerException.class);
        toAggregator(aggrOp);
    }

    @Test
    public void test_toAggregator() {
        int count = 10_000;
        putToBatchSrcMap(IntStream.range(0, count).boxed().collect(Collectors.toList()));
        Map<Integer, Long> output = srcMap.aggregate(
            toAggregator(
                groupingBy(e -> e.getValue() % 2, counting())
            )
        );
        long expected = count / 2;
        assertEquals(expected, (long) output.get(0));
        assertEquals(expected, (long) output.get(1));
    }
}
