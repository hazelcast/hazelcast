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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OrderedBatchParallelismTest {

    private static final int DEFAULT_PARALLELISM = 8;
    // Used to set the LP of the stage with the higher value than upstream parallelism
    private static final int HIGH_LOCAL_PARALLELISM = 11;
    // Used to set the LP of the stage with the smaller value than upstream parallelism
    private static final int LOW_LOCAL_PARALLELISM = 2;
    private static final int UPSTREAM_PARALLELISM = 6;
    private static final Context PIPELINE_CTX = new Context() {
        @Override public int defaultLocalParallelism() {
            return DEFAULT_PARALLELISM;
        }
    };

    @Parameter(value = 0)
    public FunctionEx<BatchStage<Long>, BatchStage<Long>> transform;

    @Parameter(value = 1)
    public List<String> vertexNames;

    @Parameter(value = 2)
    public List<Integer> expectedLPs;

    @Parameter(value = 3)
    public String transformName;

    @Parameters(name = "{index}: transform={3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map"
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(x -> Traversers.singleton(1L))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("flat-map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "flat-map"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("mapUsingIMap"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map-using-imap"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("mapUsingReplicatedMap"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map-using-replicated-map"
                ),
                createParamSet(
                        stage -> stage
                                .filter(x -> x % 2 == 0)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("filter"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "filter"
                ),
                createParamSet(
                        stage -> stage
                                .sort()
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("sort", "sort-collect"),
                        Arrays.asList(UPSTREAM_PARALLELISM, 1),
                        "sort"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("map-stateful-global"),
                        Collections.singletonList(1),
                        "map-stateful-global"
                ),
                createParamSet(
                        stage -> stage
                                .distinct()
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("distinct-prepare", "distinct"),
                        Arrays.asList(HIGH_LOCAL_PARALLELISM, HIGH_LOCAL_PARALLELISM),
                        "distinct"
                ),
                createParamSet(
                        stage -> stage
                                .aggregate(counting())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("aggregate-prepare", "aggregate"),
                        Arrays.asList(UPSTREAM_PARALLELISM, 1),
                        "two-stage-aggregation"
                ),
                createParamSet(
                        stage -> stage
                                .<Long>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("custom-transform"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "custom-transform"
                ),
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .aggregate(counting())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .flatMap(x -> Traversers.<Long>traverseItems())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("map", "aggregate-prepare", "aggregate", "flat-map"),
                        Arrays.asList(UPSTREAM_PARALLELISM, UPSTREAM_PARALLELISM, 1, 1),
                        "map+aggregate+flat-map"
                ),
                createParamSet(
                        stage -> stage
                                .peek()
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map-after-peek"
                ),
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .filter(x -> x % 2 == 0)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM)
                                .flatMap(x -> Traversers.<Long>traverseItems())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("map", "filter", "flat-map"),
                        Arrays.asList(UPSTREAM_PARALLELISM, LOW_LOCAL_PARALLELISM, LOW_LOCAL_PARALLELISM),
                        "map+filter+flat-map"
                ),
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM)
                                .filter(x -> x % 2 == 0)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .flatMap(x -> Traversers.<Long>traverseItems())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("map", "fused(filter, flat-map)"),
                        Arrays.asList(LOW_LOCAL_PARALLELISM, LOW_LOCAL_PARALLELISM),
                        "map+filter+flat-map-2"
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<BatchStage<Long>, BatchStage<Long>> transform,
            List<String> vertexNames,
            List<Integer> expectedLPs,
            String transformName
    ) {
        return new Object[]{transform, vertexNames, expectedLPs, transformName};
    }


    @Test
    public void when_transform_applied_lp_should_match_expectedLP() {
        // When
        DAG dag = applyTransformAndGetDag(transform);
        // Then
        for (int i = 0; i < vertexNames.size(); i++) {
            Vertex tsVertex = dag.getVertex(vertexNames.get(i));
            assertEquals((int) expectedLPs.get(i), tsVertex.getLocalParallelism());
            // To show ineffectiveness of Vertex.determineLocalParallelism()
            assertEquals((int) expectedLPs.get(i), tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
        }
    }

    private DAG applyTransformAndGetDag(FunctionEx<BatchStage<Long>, BatchStage<Long>> transform) {
        PipelineImpl p = (PipelineImpl) Pipeline.create().setPreserveOrder(true);
        BatchStage<Long> source = p.readFrom(TestSources.items(1L))
                                   .setLocalParallelism(UPSTREAM_PARALLELISM);
        BatchStage<Long> applied = source.apply(transform);
        applied.mapStateful(LongAccumulator::new, (s, x) -> x)
               .writeTo(Sinks.noop());
        return p.toDag(PIPELINE_CTX);
    }

}
