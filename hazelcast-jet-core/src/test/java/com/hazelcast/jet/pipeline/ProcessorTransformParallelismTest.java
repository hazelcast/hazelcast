/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.function.FunctionEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM;
import static com.hazelcast.jet.pipeline.ContextFactory.withCreateFn;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class ProcessorTransformParallelismTest {

    private static final int DEFAULT_PARALLELISM = 8;
    private static final int LOCAL_PARALLELISM = 11;

    private static final ContextFactory<Object> CONTEXT_FACTORY = withCreateFn(x -> null);
    private static final ContextFactory<Object> NC_CONTEXT_FACTORY = CONTEXT_FACTORY.toNonCooperative();

    @Parameter(value = 0)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperative_defaultLP;

    @Parameter(value = 1)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperative_explicitLP;

    @Parameter(value = 2)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> nonCooperative_defaultLP;

    @Parameter(value = 3)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> nonCooperative_explicitLP;

    @Parameter(value = 4)
    public String transformName;

    @Parameters(name = "{index}: transform={4}")
    @SuppressWarnings(value = {"checkstyle:LineLength", "checkstyle:MethodLength"})
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .mapUsingContext(CONTEXT_FACTORY, (c, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingContext(CONTEXT_FACTORY, (c, t) -> t),
                        stage -> stage
                                .mapUsingContext(NC_CONTEXT_FACTORY, (c, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingContext(NC_CONTEXT_FACTORY, (c, t) -> t),
                        "mapUsingContext"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContext(CONTEXT_FACTORY, (c, k, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContext(CONTEXT_FACTORY, (c, k, t) -> t),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> t),
                        "mapUsingPartitionedContext"),
                createParamSet(
                        stage -> stage
                                .mapUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> t)),
                        stage -> stage
                                .mapUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> t)),
                        "mapUsingContextAsync"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> t)),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> t)),
                        "mapUsingPartitionedContextAsync"),
                createParamSet(
                        stage -> stage
                                .filterUsingContext(CONTEXT_FACTORY, (c, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingContext(CONTEXT_FACTORY, (c, t) -> false),
                        stage -> stage
                                .filterUsingContext(NC_CONTEXT_FACTORY, (c, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingContext(NC_CONTEXT_FACTORY, (c, t) -> false),
                        "filterUsingContext"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContext(CONTEXT_FACTORY, (c, k, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContext(CONTEXT_FACTORY, (c, k, t) -> false),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> false),
                        "filterUsingPartitionedContext"),
                createParamSet(
                        stage -> stage
                                .filterUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> false))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> false)),
                        stage -> stage
                                .filterUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> false))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(() -> false)),
                        "filterUsingContextAsync"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> false))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> false)),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> false))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(() -> false)),
                        "filterUsingPartitionedContextAsync"),
                createParamSet(
                        stage -> stage
                                .flatMapUsingContext(CONTEXT_FACTORY, (c, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingContext(CONTEXT_FACTORY, (c, t) -> Traversers.empty()),
                        stage -> stage
                                .flatMapUsingContext(NC_CONTEXT_FACTORY, (c, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingContext(NC_CONTEXT_FACTORY, (c, t) -> Traversers.empty()),
                        "flatMapUsingContext"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContext(CONTEXT_FACTORY, (c, k, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContext(CONTEXT_FACTORY, (c, k, t) -> Traversers.empty()),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContext(NC_CONTEXT_FACTORY, (c, k, t) -> Traversers.empty()),
                        "flatMapUsingPartitionedContext"),
                createParamSet(
                        stage -> stage
                                .flatMapUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(Traversers::<Integer>empty))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingContextAsync(CONTEXT_FACTORY, (c, t) -> supplyAsync(Traversers::empty)),
                        stage -> stage
                                .flatMapUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(Traversers::<Integer>empty))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingContextAsync(NC_CONTEXT_FACTORY, (c, t) -> supplyAsync(Traversers::empty)),
                        "flatMapUsingContextAsync"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(Traversers::<Integer>empty))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContextAsync(CONTEXT_FACTORY, (c, k, t) -> supplyAsync(Traversers::empty)),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(Traversers::<Integer>empty))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingContextAsync(NC_CONTEXT_FACTORY, (c, k, t) -> supplyAsync(Traversers::empty)),
                        "flatMapUsingPartitionedContextAsync")
        );
    }

    private static Object[] createParamSet(
            FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperative_defaultLP,
            FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperative_explicitLP,
            FunctionEx<StreamStage<Integer>, StreamStage<Integer>> nonCooperative_defaultLP,
            FunctionEx<StreamStage<Integer>, StreamStage<Integer>> nonCooperative_explicitLP,
            String transformName
    ) {
        return new Object[]{cooperative_defaultLP, cooperative_explicitLP, nonCooperative_defaultLP,
                nonCooperative_explicitLP, transformName};
    }

    @Test
    public void when_cooperative_defaultLP_then_UsesProvidedLP() {
        // When
        DAG dag = applyTransformAndGetDag(cooperative_defaultLP);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(LOCAL_PARALLELISM, tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
    }

    @Test
    public void when_cooperative_explicitLP_then_UsesDefaultLP() {
        // When
        DAG dag = applyTransformAndGetDag(cooperative_explicitLP);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(DEFAULT_PARALLELISM, tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
    }

    @Test
    public void when_nonCooperative_defaultLP_then_UsesProvidedLP() {
        // When
        DAG dag = applyTransformAndGetDag(nonCooperative_defaultLP);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(LOCAL_PARALLELISM, tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
    }

    @Test
    public void when_nonCooperative_explicitLP_then_UsesDefaultLP() {
        // When
        DAG dag = applyTransformAndGetDag(nonCooperative_explicitLP);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM, tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
    }

    private DAG applyTransformAndGetDag(FunctionEx<StreamStage<Integer>, StreamStage<Integer>> transform) {
        Pipeline p = Pipeline.create();
        StreamStage<Integer> source = p.drawFrom(TestSources.items(1))
                                       .addTimestamps(t -> 0, 0);
        StreamStage<Integer> applied = source.apply(transform);
        applied.drainTo(Sinks.noop());
        return p.toDag();
    }
}
