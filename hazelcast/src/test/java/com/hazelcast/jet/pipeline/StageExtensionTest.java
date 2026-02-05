/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class StageExtensionTest extends PipelineStreamTestSupport {

    private static final int GROUPING_KEYS = 10;

    @Test
    public void setName() {
        // When
        var srcStage = streamStageFromList(emptyList())
                .setName("source");
        var incrementStage = srcStage.using(IncrementingExtension::new)
                .increment().setName("increment");

        // Then
        assertThat(srcStage.name())
                .as("upstream should be unchanged")
                .isEqualTo("source");
        assertThat(incrementStage.name())
                .as("new stage should be configured")
                .isEqualTo("increment");
    }

    @Test
    public void setLocalParallelism() {
        // Given
        int localParallelism = 10;

        // When
        var srcStage = batchStageFromList(emptyList())
                .setLocalParallelism(localParallelism);
        var incrementStage = srcStage.using(IncrementingExtension::new)
                .increment().setLocalParallelism(localParallelism * 2);

        // Then
        assertThat(transformOf(srcStage).localParallelism())
                .as("upstream should be unchanged")
                .isEqualTo(localParallelism);
        assertThat(transformOf(incrementStage).localParallelism())
                .as("new stage should be configured")
                .isEqualTo(2 * localParallelism);
    }

    @Test
    public void streamStage() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);

        srcStage.using(IncrementingExtension::new)
                .increment()
                .writeTo(sinkList());
        execute();

        assertThatIncrementedAllItems();
    }

    @Test
    public void streamStageWithKey() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);

        srcStage.groupingKey(e -> e % GROUPING_KEYS)
                .using(IncrementingKeyedExtension::new)
                .increment()
                .writeTo(sinkList());
        execute();

        assertThatIncrementedAllItems();
    }

    @Test
    public void batchStage() {
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);

        srcStage.using(IncrementingExtension::new)
                .increment()
                .writeTo(sinkList());
        execute();

        assertThatIncrementedAllItems();
    }

    @Test
    public void batchStageWithKey() {
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);

        srcStage.groupingKey(e -> e % GROUPING_KEYS)
                .using(IncrementingKeyedExtension::new)
                .increment()
                .writeTo(sinkList());
        execute();

        assertThatIncrementedAllItems();
    }

    private void assertThatIncrementedAllItems() {
        assertThat(sinkList)
                .hasSize(itemCount)
                .as("should contain all entries incremented")
                .containsAll(sequence(itemCount).stream().map(x -> x + 1).toList());
    }

    static class IncrementingExtension<S extends GeneralStage<Integer>> {
        private final S baseStage;

        IncrementingExtension(S baseStage) {
            this.baseStage = baseStage;
        }

        @SuppressWarnings("unchecked")
        S increment() {
            return (S) baseStage.map(x -> 1 + x);
        }
    }

    static class IncrementingKeyedExtension<S extends GeneralStageWithKey<Integer, Integer>> {
        private final S baseStage;

        IncrementingKeyedExtension(S baseStage) {
            this.baseStage = baseStage;
        }

        GeneralStage<Integer> increment() {
            // keyed stages do not have simple map transform, simulate it using dummy mapStateful
            return baseStage.mapStateful(() -> null, (__, ___, x) -> x + 1);
        }
    }
}
