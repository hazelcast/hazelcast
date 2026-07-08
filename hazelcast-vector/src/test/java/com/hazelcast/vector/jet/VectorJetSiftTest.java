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

import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.internal.impl.Hints;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
public class VectorJetSiftTest extends HazelcastTestSupport {
    private static final String SIFT_SMALL_PATH = "src/test/resources/siftsmall";

    private HazelcastInstance[] instances;
    private VectorCollection<Integer, Integer> collection;
    private IMap<Integer, Set<Integer>> gtMap;
    private String currentDir;

    private HazelcastInstance hz() {
        return instances[0];
    }

    @Before
    public void setup() {
        currentDir = Paths.get(".").toAbsolutePath().normalize().toString();
        // set parameters to get recall < 1
        instances = createHazelcastInstances(smallInstanceConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1"),
                1);
        var vectorCollectionConfig = new VectorCollectionConfig("siftsmall")
                .addVectorIndexConfig(new VectorIndexConfig("sift-small-index", Metric.EUCLIDEAN, 128));
        hz().getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        collection = hz().getVectorCollection(vectorCollectionConfig.getName());
        gtMap = hz().getMap("siftsmall_groundtruth");
    }

    @Test
    public void testRecallSiftSmall() throws ExecutionException, InterruptedException {
        assertThat(testRecallSiftSmall(null))
                .as("With default efSearch the recall should be slightly smaller than 1")
                .isStrictlyBetween(0.99, 1d);
    }

    @Test
    public void testRecallSiftSmallWithEfSearch() throws ExecutionException, InterruptedException {
        assertThat(testRecallSiftSmall(256))
                .as("With increased efSearch the recall should be perfect")
                .isOne();
    }

    private double testRecallSiftSmall(Integer efSearch) throws ExecutionException, InterruptedException {
        final int topK = 100;

        var load = Pipeline.create();
        // use unified file connector
        load.readFrom(FileSources.files(currentDir + "/" + SIFT_SMALL_PATH)
                        .glob("siftsmall_base.fvecs")
                        .format(VectorSources.fvecsFormat())
                        .build())
                    .writeTo(VectorSinks.vectorCollection(collection,
                            // key
                            Map.Entry::getKey,
                            // value
                            Map.Entry::getKey,
                            // vectors
                            Map.Entry::getValue));
        var loadJob = hz().getJet().newJob(load);

        var loadGroundTruth = Pipeline.create();
        loadGroundTruth.readFrom(VectorSources.ivecs(SIFT_SMALL_PATH, "siftsmall_groundtruth.ivecs"))
                .writeTo(Sinks.map(gtMap, Map.Entry::getKey, e -> toSet(e.getValue())));
        var loadGtJob = hz().getJet().newJob(loadGroundTruth);

        // load in parallel
        loadJob.join();
        loadGtJob.join();

        // measure recall
        Observable<Double> recall = hz().getJet().newObservable();
        var search = Pipeline.create();
        // use legacy file connector
        SearchOptionsBuilder optionsBuilder = SearchOptions.builder()
                .limit(topK);
        if (efSearch != null) {
            optionsBuilder.hint(Hints.EF_SEARCH, efSearch);
        }
        var recallStage = search.readFrom(VectorSources.fvecs(SIFT_SMALL_PATH, "siftsmall_query.fvecs"))
                .apply(VectorTransforms.mapUsingVectorSearchBatch(collection,
                        optionsBuilder.build(),
                        // vector
                        Map.Entry::getValue,
                        // process search results
                        (input, result) -> Map.entry(input.getKey(),
                                Lists.newArrayList(IterableUtil.map(result.results(), sr -> (Integer) sr.getKey())))))
                // compare results with ground truth
                .mapUsingIMap(gtMap, Map.Entry::getKey,
                        (result, gt) -> IntStream.range(0, topK).filter(j -> gt.contains(result.getValue().get(j))).count())
                // average
                .aggregate(AggregateOperations.averagingDouble(l -> (double) l / topK));
        // log the recall and return it via observable
        recallStage.writeTo(Sinks.logger(r -> "Recall: " + r));
        recallStage.writeTo(Sinks.observable(recall));

        hz().getJet().newJob(search).join();

        // return calculated recall
        return recall.toFuture(s -> s.collect(MoreCollectors.onlyElement())).get();
    }

    @Test
    public void ivecsLegacyAndUnifiedConnectorAreEquivalent() {
        var loadGroundTruthLegacy = Pipeline.create();
        loadGroundTruthLegacy.readFrom(VectorSources.ivecs(SIFT_SMALL_PATH, "siftsmall_groundtruth.ivecs"))
                .writeTo(Sinks.map("legacy-gt"));
        var loadGtLegacy = hz().getJet().newJob(loadGroundTruthLegacy);

        var loadGroundTruthUnified = Pipeline.create();
        loadGroundTruthUnified.readFrom(FileSources.files(currentDir + "/" + SIFT_SMALL_PATH)
                .glob("siftsmall_groundtruth.ivecs")
                .format(VectorSources.ivecsFormat())
                .build())
                .writeTo(Sinks.map("unified-gt"));
        var loadGtUnified = hz().getJet().newJob(loadGroundTruthUnified);

        loadGtLegacy.join();
        loadGtUnified.join();

        assertThat((Map<?, ?>) hz().getMap("legacy-gt"))
                .containsExactlyInAnyOrderEntriesOf(hz().getMap("unified-gt"));
    }

    private static Set<Integer> toSet(int[] values) {
        Set<Integer> set = new HashSet<>(values.length);
        Arrays.stream(values).forEach(set::add);
        return set;
    }
}
