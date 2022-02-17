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

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OrderedStreamParallelismTest {

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
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> transform;

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
                                .flatMap(x -> Traversers.singleton(1))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("flat-map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "flat-map"
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
                                .peek()
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Collections.singletonList("map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map-after-peek"
                ),
                createParamSet(
                        stage -> stage
                                .peek()
                                .map(x -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM)
                                .flatMap(x -> Traversers.singleton(1))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        Arrays.asList("map", "flat-map"),
                        Arrays.asList(LOW_LOCAL_PARALLELISM, LOW_LOCAL_PARALLELISM),
                        "map+flat-map"
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<StreamStage<Integer>, StreamStage<Integer>> transform,
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
        System.out.println(dag.toJson(DEFAULT_PARALLELISM));
        // Then
        for (int i = 0; i < vertexNames.size(); i++) {
            Vertex tsVertex = dag.getVertex(vertexNames.get(i));
            assertEquals((int) expectedLPs.get(i), tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
        }
    }

    private DAG applyTransformAndGetDag(FunctionEx<StreamStage<Integer>, StreamStage<Integer>> transform) {
        PipelineImpl p = (PipelineImpl) Pipeline.create().setPreserveOrder(true);
        StreamStage<Integer> source = p.readFrom(TestSources.items(1))
                                       .setLocalParallelism(UPSTREAM_PARALLELISM)
                                       .addTimestamps(t -> 0, Long.MAX_VALUE);
        StreamStage<Integer> applied = source.apply(transform);
        applied.mapStateful(LongAccumulator::new, (s, x) -> x)
               .writeTo(Sinks.noop());
        return p.toDag(PIPELINE_CTX);
    }
}
