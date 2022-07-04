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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_MAX_CONCURRENT_OPS;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsyncTransformUsingServiceP_IntegrationTest extends SimpleTestInClusterSupport {

    private static final int NUM_ITEMS = 100;

    @Parameter
    public boolean ordered;

    private IMap<Integer, Integer> journaledMap;
    private ServiceFactory<?, ExecutorService> serviceFactory;
    private IList<Object> sinkList;
    private JobConfig jobConfig;

    @Parameters(name = "ordered={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getMapConfig("journaledMap*")
              .getEventJournalConfig()
              .setEnabled(true)
              .setCapacity(100_000);

        initialize(1, config);
    }

    @Before
    public void before() {
        journaledMap = instance().getMap(randomMapName("journaledMap"));
        journaledMap.putAll(IntStream.range(0, NUM_ITEMS).boxed().collect(toMap(i -> i, i -> i)));
        sinkList = instance().getList(randomMapName("sinkList"));
        jobConfig = new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(0);
        serviceFactory = sharedService(pctx -> Executors.newFixedThreadPool(8), ExecutorService::shutdown);
    }

    @Test
    public void stressTest_noRestart() {
        stressTestInt(false);
    }

    @Test
    public void stressTest_withRestart_graceful() {
        stressTestInt(true);
    }

    private void stressTestInt(boolean restart) {
        /*
        This is a stress test of the cooperative emission using the DAG api. Only through DAG
        API we can configure edge queue sizes, which we use to cause more trouble for the
        cooperative emission.
         */

        // add more input to the source map
        int numItems = 10_000;
        journaledMap.putAll(IntStream.range(NUM_ITEMS, numItems).boxed().collect(toMap(i -> i, i -> i)));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", throttle(streamMapP(journaledMap.getName(), alwaysTrue(),
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, eventTimePolicy(
                        i -> (long) ((Integer) i),
                        WatermarkPolicy.limitingLag(10),
                        10, 0, 0
                )), 5000));
        BiFunctionEx<ExecutorService, Integer, CompletableFuture<Traverser<String>>> flatMapAsyncFn =
                transformNotPartitionedFn(i -> traverseItems(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"));
        ProcessorSupplier processorSupplier = ordered
                ? AsyncTransformUsingServiceOrderedP.supplier(
                        serviceFactory, DEFAULT_MAX_CONCURRENT_OPS, flatMapAsyncFn)
                : AsyncTransformUsingServiceUnorderedP.supplier(
                        serviceFactory, DEFAULT_MAX_CONCURRENT_OPS, flatMapAsyncFn, identity());
        Vertex map = dag.newVertex("map", processorSupplier).localParallelism(2);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(sinkList.getName()));

        // Use a shorter queue to not block the barrier from the source for too long due to
        // the backpressure from the slow mapper
        EdgeConfig edgeToMapperConfig = new EdgeConfig().setQueueSize(128);
        // Use a shorter queue on output from the mapper so that we experience backpressure
        // from the sink
        EdgeConfig edgeFromMapperConfig = new EdgeConfig().setQueueSize(10);
        dag.edge(between(source, map).setConfig(edgeToMapperConfig))
           .edge(between(map, sink).setConfig(edgeFromMapperConfig));

        Job job = instance().getJet().newJob(dag, jobConfig);
        for (int i = 0; restart && i < 5; i++) {
            assertJobStatusEventually(job, RUNNING);
            sleepMillis(100);
            job.restart();
        }
        assertResultEventually(i -> Stream.of(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"), numItems);
    }

    @Test
    public void test_pipelineApi_mapNotPartitioned() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(journaledMap, START_FROM_OLDEST, EventJournalMapEvent::getNewValue, alwaysTrue()))
         .withoutTimestamps()
         .mapUsingServiceAsync(serviceFactory, transformNotPartitionedFn(i -> i + "-1"))
         .setLocalParallelism(2)
         .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p, jobConfig);
        assertResultEventually(i -> Stream.of(i + "-1"), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_mapPartitioned() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(journaledMap, START_FROM_OLDEST, EventJournalMapEvent::getNewValue, alwaysTrue()))
         .withoutTimestamps()
         .groupingKey(i -> i % 10)
         .mapUsingServiceAsync(serviceFactory, transformPartitionedFn(i -> i + "-1"))
         .setLocalParallelism(2)
         .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p, jobConfig);
        assertResultEventually(i -> Stream.of(i + "-1"), NUM_ITEMS);
    }

    private <R> BiFunctionEx<ExecutorService, Integer, CompletableFuture<R>> transformNotPartitionedFn(
            FunctionEx<Integer, R> transformFn
    ) {
        return (executor, item) -> {
            CompletableFuture<R> f = new CompletableFuture<>();
            executor.submit(() -> {
                // simulate random async call latency
                sleepMillis(ThreadLocalRandom.current().nextInt(5));
                return f.complete(transformFn.apply(item));
            });
            return f;
        };
    }

    private <R> TriFunction<ExecutorService, Integer, Integer, CompletableFuture<R>> transformPartitionedFn(
            FunctionEx<Integer, R> transformFn
    ) {
        return (executor, key, item) -> {
            assert key == item % 10 : "item=" + item + ", key=" + key;
            CompletableFuture<R> f = new CompletableFuture<>();
            executor.submit(() -> {
                // simulate random async call latency
                sleepMillis(ThreadLocalRandom.current().nextInt(5));
                return f.complete(transformFn.apply(item));
            });
            return f;
        };
    }

    private void assertResultEventually(Function<Integer, Stream<? extends String>> transformFn, int numItems) {
        String expected = IntStream.range(0, numItems)
                                   .boxed()
                                   .flatMap(transformFn)
                                   .sorted()
                                   .collect(joining("\n"));
        assertTrueEventually(() -> assertEquals(expected, sinkList.stream().map(Object::toString).sorted()
                                                                  .collect(joining("\n"))), 240);
    }
}
