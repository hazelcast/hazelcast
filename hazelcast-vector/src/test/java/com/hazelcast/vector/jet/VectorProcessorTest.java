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

package com.hazelcast.vector.jet;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.SearchResultsImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.vector.impl.VectorTestUtils.sr;
import static com.hazelcast.vector.impl.VectorTestUtils.srs;
import static com.hazelcast.vector.impl.VectorTestUtils.usingOverriddenEqualsIgnoringFields;
import static com.hazelcast.vector.impl.VectorTestUtils.vec;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
public class VectorProcessorTest extends HazelcastTestSupport {
    private static final int ENTRY_COUNT = 100;

    private final String collectionName = randomName();
    private final String indexName = randomName();
    private HazelcastInstance[] instances;
    private VectorCollection<Integer, Integer> collection;

    @Before
    public void setup() {
        instances = createHazelcastInstances(smallInstanceConfig(), 2);
        collection = getVectorCollectionWith1Dim(instances[0]);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            collection.putAsync(i, VectorDocument.of(i, vec(i * 0.1f))).toCompletableFuture().join();
        }
    }

    @Test
    public void batchPipelineShouldSearchData() {
        batchPipelineShouldSearchData(i -> vec(i * 0.1f));
    }

    @Test
    public void batchPipelineShouldSearchDataUsingNamedIndex() {
        final String finalIndexName = indexName;
        batchPipelineShouldSearchData(i -> VectorValues.of(finalIndexName, new float[]{i * 0.1f}));
    }

    private void batchPipelineShouldSearchData(FunctionEx<Integer, VectorValues> toVectorFn) {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemsDistributed(List.of(IntStream.range(0, 100).boxed().toArray(Integer[]::new))))
                .apply(VectorTransforms.mapUsingVectorSearchBatch(collectionName,
                        SearchOptions.of(1, true, true),
                        toVectorFn,
                        Map::entry))
                .writeTo(Sinks.map("results"));

        var job = instances[0].getJet().newJob(p);
        job.join();

        Map<Integer, SearchResultsImpl> resultsIMap = instances[0].getMap("results");
        assertThat(resultsIMap).hasSize(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            assertThat(resultsIMap.get(i))
                    .usingRecursiveComparison(usingOverriddenEqualsIgnoringFields("id"))
                    .isEqualTo(srs(sr(i, 1.0f, i, vec(i * 0.1f))));
        }
    }

    @Test
    public void streamPipelineShouldSearchData() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(100)).withoutTimestamps()
                .map(se -> (int) se.sequence())
                .apply(VectorTransforms.mapUsingVectorSearch(collectionName,
                        SearchOptions.of(1, true, true),
                        i -> vec(i * 0.1f),
                        Map::entry))
                // remove results past assertion limit
                .filter(e -> e.getKey() < ENTRY_COUNT)
                .writeTo(Sinks.map("results"));

        var job = instances[0].getJet().newJob(p);

        Map<Integer, SearchResultsImpl> resultsIMap = instances[0].getMap("results");

        assertTrueEventually(() -> {
            assertThat(resultsIMap).hasSize(ENTRY_COUNT);
            for (int i = 0; i < ENTRY_COUNT; ++i) {
                assertThat(resultsIMap.get(i))
                        .usingRecursiveComparison(usingOverriddenEqualsIgnoringFields("id"))
                        .isEqualTo(srs(sr(i, 1.0f, i, vec(i * 0.1f))));
            }
        });

        job.cancel();
    }

    private <T> VectorCollection<T, T> getVectorCollectionWith1Dim(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setName(indexName)
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }
}
