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

package com.hazelcast.jet.pipeline;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SortBatchStageTest extends PipelineTestSupport {

    @Test
    public void sort_withRepeatingItems() {
        // Given
        List<Integer> input = IntStream.range(0, itemCount).map(t -> t / 10).boxed().collect(toList());
        List<Integer> expected = new ArrayList<>(input);
        Collections.shuffle(input);

        // When
        BatchStage<Integer> sorted = batchStageFromList(input).rebalance().sort();

        // Then
        sorted.writeTo(assertOrdered(expected));
        execute();
    }

    @Test
    public void sort_withComparator() {
        // Given
        List<Integer> input = IntStream.range(0, itemCount).boxed().sorted(Collections.reverseOrder()).collect(toList());
        List<Integer> expected = new ArrayList<>(input);
        Collections.shuffle(input);

        // When
        BatchStage<Integer> sorted = batchStageFromList(input).rebalance()
                .sort((a, b) -> b - a);

        // Then
        sorted.writeTo(assertOrdered(expected));
        execute();
    }

    @Test
    public void sort_exceptionDuringComparing() {
        // Given
        List<Integer> input = IntStream.range(0, 20).boxed().collect(toList());

        // When
        BatchStage<Integer> sorted = batchStageFromList(input).rebalance()
                .sort((a, b) -> {
                    if (a == 5 || b == 5) {
                        throw new RuntimeException("expectedFailure");
                    }
                    return a - b;
                });
        sorted.writeTo(Sinks.logger());

        // Then
        assertThatThrownBy(this::execute)
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("expectedFailure");
    }

    @Test
    public void sort_lessItemsThanProcessors() {
        // Given
        List<Integer> input = IntStream.range(0, 3).boxed().collect(toList());
        List<Integer> expected = new ArrayList<>(input);
        Collections.shuffle(input);

        // When
        BatchStage<Integer> sorted = batchStageFromList(input).sort();

        // Then
        sorted.writeTo(assertOrdered(expected));
        execute();
    }
}
