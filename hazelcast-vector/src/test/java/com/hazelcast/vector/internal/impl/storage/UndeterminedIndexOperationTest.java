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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.vector.internal.impl.GraphRepresentationDataOutput;
import com.hazelcast.vector.internal.impl.VectorTestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import static com.hazelcast.config.vector.Metric.EUCLIDEAN;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVectorFloat;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test aims to show that the index optimization operation is non-deterministic, producing different results on identical graphs.
 */
@Ignore
public class UndeterminedIndexOperationTest {

    private static final int dimension = 100;
    private final Supplier<Data> generateKey = () -> new HeapData(UuidUtil.newUnsecureUuidString().getBytes());
    private AbstractVectorIndex vectorIndex1;
    private AbstractVectorIndex vectorIndex2;

    @Before
    public void generateIdenticalIndexes() {
        vectorIndex1 = createIndex(UUID.randomUUID().toString());
        vectorIndex2 = createIndex(UUID.randomUUID().toString());

        int indexSize = 20_000;
        range(0, indexSize).forEach(
                i -> {
                    var key = generateKey.get();
                    var vector = randomVectorFloat(dimension);
                    vectorIndex1.put(key, vector);
                    vectorIndex2.put(key, vector);
                }
        );
    }

    private static AbstractVectorIndex createIndex(String name) {
        return new VectorIndexSingleKey(
                name,
                EUCLIDEAN,
                16,
                100,
                dimension,
                ForkJoinPool.commonPool()
        );
    }

    // Optimization results in identical graphs becoming different.
    @Test
    public void optimizeIdenticalGraph_then_differentGraph() {

        GraphRepresentationDataOutput graphRepresentationBeforeOptimization1 = new GraphRepresentationDataOutput();
        GraphRepresentationDataOutput graphRepresentationBeforeOptimization2 = new GraphRepresentationDataOutput();

        vectorIndex1.indexBuilder.getGraph().save(graphRepresentationBeforeOptimization1);
        vectorIndex2.indexBuilder.getGraph().save(graphRepresentationBeforeOptimization2);

        // graphs are equals before optimization
        assertThat(graphRepresentationBeforeOptimization1.getGraph()).isEqualTo(graphRepresentationBeforeOptimization2.getGraph());

        cleanup(vectorIndex1);
        cleanup(vectorIndex2);

        GraphRepresentationDataOutput graphRepresentationAfterOptimization1 = new GraphRepresentationDataOutput();
        GraphRepresentationDataOutput graphRepresentationAfterOptimization2 = new GraphRepresentationDataOutput();

        vectorIndex1.indexBuilder.getGraph().save(graphRepresentationAfterOptimization1);
        vectorIndex2.indexBuilder.getGraph().save(graphRepresentationAfterOptimization2);

        // graphs are not equals after optimization
        assertThat(graphRepresentationAfterOptimization1.getGraph()).isNotEqualTo(graphRepresentationAfterOptimization2.getGraph());
    }

    // Optimization results in identical index becoming different.
    @Test
    public void optimizeIdenticalIndex_then_differentSearchResults() {
        cleanup(vectorIndex1);
        cleanup(vectorIndex2);

        assertThatThrownBy(() -> {
            for (int topK = 1; topK < 1_000; topK++) {
                var searchVector = randomVectorFloat(dimension);

                var result1 = vectorIndex1.search(searchVector, topK);
                var map1 = VectorTestUtils.toMap(result1);

                var result2 = vectorIndex2.search(searchVector, topK);
                var map2 = VectorTestUtils.toMap(result2);

                assertThat(map1).as("Failed on topK = %s", topK).isEqualTo(map2);
            }
        });
    }

    private void cleanup(AbstractVectorIndex vectorIndex) {
        try {
            vectorIndex.lockIndexMutation(UUID.randomUUID());
            vectorIndex.cleanup();
        } finally {
            vectorIndex.unlockIndexMutation();
        }
    }
}
