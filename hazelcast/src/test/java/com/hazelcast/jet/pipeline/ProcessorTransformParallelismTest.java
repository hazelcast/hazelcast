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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
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

import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_MAX_CONCURRENT_OPS;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_PRESERVE_ORDER;
import static com.hazelcast.jet.pipeline.ServiceFactory.withCreateContextFn;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorTransformParallelismTest {

    private static final int DEFAULT_PARALLELISM = 8;
    private static final int LOCAL_PARALLELISM = 11;

    private static final ServiceFactory<Void, Void> SERVICE_FACTORY = withCreateContextFn(x -> null);
    private static final ServiceFactory<Void, Void> NC_SERVICE_FACTORY = SERVICE_FACTORY.toNonCooperative();

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
                                .mapUsingService(SERVICE_FACTORY, (c, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingService(SERVICE_FACTORY, (c, t) -> t),
                        stage -> stage
                                .mapUsingService(NC_SERVICE_FACTORY, (c, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingService(NC_SERVICE_FACTORY, (c, t) -> t),
                        "mapUsingService"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingService(SERVICE_FACTORY, (c, k, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingService(SERVICE_FACTORY, (c, k, t) -> t),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingService(NC_SERVICE_FACTORY, (c, k, t) -> t)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingService(NC_SERVICE_FACTORY, (c, k, t) -> t),
                        "mapUsingPartitionedService"),
                createParamSet(
                        stage -> stage
                                .mapUsingServiceAsync(SERVICE_FACTORY, (c, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingServiceAsync(SERVICE_FACTORY, (c, t) -> supplyAsync(() -> t)),
                        stage -> stage
                                .mapUsingServiceAsync(NC_SERVICE_FACTORY, (c, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .mapUsingServiceAsync(NC_SERVICE_FACTORY, (c, t) -> supplyAsync(() -> t)),
                        "mapUsingServiceAsync"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingServiceAsync(SERVICE_FACTORY, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER,
                                        (c, k, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingServiceAsync(SERVICE_FACTORY, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER,
                                        (c, k, t) -> supplyAsync(() -> t)),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingServiceAsync(NC_SERVICE_FACTORY, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER,
                                        (c, k, t) -> supplyAsync(() -> t))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .mapUsingServiceAsync(NC_SERVICE_FACTORY, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER,
                                        (c, k, t) -> supplyAsync(() -> t)),
                        "mapUsingPartitionedServiceAsync"),
                createParamSet(
                        stage -> stage
                                .filterUsingService(SERVICE_FACTORY, (c, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingService(SERVICE_FACTORY, (c, t) -> false),
                        stage -> stage
                                .filterUsingService(NC_SERVICE_FACTORY, (c, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .filterUsingService(NC_SERVICE_FACTORY, (c, t) -> false),
                        "filterUsingService"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingService(SERVICE_FACTORY, (c, k, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingService(SERVICE_FACTORY, (c, k, t) -> false),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingService(NC_SERVICE_FACTORY, (c, k, t) -> false)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .filterUsingService(NC_SERVICE_FACTORY, (c, k, t) -> false),
                        "filterUsingPartitionedService"),
                createParamSet(
                        stage -> stage
                                .flatMapUsingService(SERVICE_FACTORY, (c, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingService(SERVICE_FACTORY, (c, t) -> Traversers.empty()),
                        stage -> stage
                                .flatMapUsingService(NC_SERVICE_FACTORY, (c, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .flatMapUsingService(NC_SERVICE_FACTORY, (c, t) -> Traversers.empty()),
                        "flatMapUsingService"),
                createParamSet(
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingService(SERVICE_FACTORY, (c, k, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingService(SERVICE_FACTORY, (c, k, t) -> Traversers.empty()),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingService(NC_SERVICE_FACTORY, (c, k, t) -> Traversers.<Integer>empty())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        stage -> stage
                                .groupingKey(i -> i)
                                .flatMapUsingService(NC_SERVICE_FACTORY, (c, k, t) -> Traversers.empty()),
                        "flatMapUsingPartitionedService")
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
        StreamStage<Integer> source = p.readFrom(TestSources.items(1))
                .addTimestamps(t -> 0, 0);
        StreamStage<Integer> applied = source.apply(transform);
        applied.writeTo(Sinks.noop());
        return p.toDag();
    }
}
