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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextAsyncP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class AsyncTransformUsingContextP_IntegrationTest extends JetTestSupport {

    private static final int NUM_ITEMS = 100;

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance inst;

    @Parameter
    public boolean ordered;

    private IMapJet<Integer, Integer> journaledMap;
    private ContextFactory<ExecutorService> contextFactory;
    private IListJet<Object> sinkList;
    private JobConfig jobConfig;

    @Parameters(name = "ordered={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName("journaledMap*")
                .setCapacity(100_000));
        inst = factory.newMember(config);
        factory.newMember(config);
    }

    @AfterClass
    public static void afterClass() {
        factory.terminateAll();
    }

    @Before
    public void before() {
        journaledMap = inst.getMap(randomMapName("journaledMap"));
        journaledMap.putAll(IntStream.range(0, NUM_ITEMS).boxed().collect(toMap(i -> i, i -> i)));
        sinkList = inst.getList(randomMapName("sinkList"));
        jobConfig = new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(0);

        contextFactory = ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8)).withLocalSharing();
        if (!ordered) {
            contextFactory = contextFactory.withUnorderedAsyncResponses();
        }
    }

    @After
    public void after() {
        journaledMap.destroy();
        sinkList.destroy();
        for (Job job : inst.getJobs()) {
            assertTrueEventually(() -> {
                logger.info("Cancelling job " + idToString(job.getId()));
                try {
                    job.cancel();
                } catch (Exception e) {
                    logger.warning("Failed to cancel the job, will retry", e);
                }
                assertJobStatusEventually(job, FAILED, 3);
            });
        }
    }

    @Test
    public void stressTest_noRestart() {
        stressTestInt(false);
    }

    @Test
    public void stressTest_withRestart() {
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
        Vertex map = dag.newVertex("map",
                flatMapUsingContextAsyncP(contextFactory, identity(), transformNotPartitionedFn(
                        item -> traverseItems(item + "-1", item + "-2", item + "-3", item + "-4", item + "-5"))))
                .localParallelism(2);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(sinkList.getName()));

        // Use a shorter queue to not block the barrier from the source for too long due to
        // the backpressure from the slow mapper
        EdgeConfig edgeToMapperConfig = new EdgeConfig().setQueueSize(128);
        // Use a shorter queue on output from the mapper so that we experience backpressure
        // from the sink
        EdgeConfig edgeFromMapperConfig = new EdgeConfig().setQueueSize(10);
        dag.edge(between(source, map).setConfig(edgeToMapperConfig))
           .edge(between(map, sink).setConfig(edgeFromMapperConfig));

        Job job = inst.newJob(dag, jobConfig);
        for (int i = 0; restart && i < 5; i++) {
            assertNotNull(job);
            assertTrueEventually(() -> {
                JobStatus status = job.getStatus();
                assertTrue("status=" + status, status == RUNNING || status == COMPLETED);
            });
            sleepMillis(100);
            try {
                job.restart();
            } catch (IllegalStateException e) {
                assertTrue(e.toString(), e.getMessage().startsWith("Cannot RESTART_GRACEFUL"));
                break;
            }
        }
        assertResult(i -> Stream.of(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"), numItems);
    }

    @Test
    public void test_pipelineApi_flatMapNotPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .flatMapUsingContextAsync(contextFactory,
                 transformNotPartitionedFn(i -> traverseItems(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5")))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> Stream.of(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_mapNotPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .mapUsingContextAsync(contextFactory,
                 transformNotPartitionedFn(i -> i + "-1"))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> Stream.of(i + "-1"), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_filterNotPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .filterUsingContextAsync(contextFactory,
                 transformNotPartitionedFn(i -> i % 2 == 0))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> i % 2 == 0 ? Stream.of(i + "") : Stream.empty(), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_flatMapPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .groupingKey(i -> i % 10)
         .flatMapUsingContextAsync(contextFactory,
                 transformPartitionedFn(i -> traverseItems(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5")))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> Stream.of(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_mapPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .groupingKey(i -> i % 10)
         .mapUsingContextAsync(contextFactory,
                 transformPartitionedFn(i -> i + "-1"))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> Stream.of(i + "-1"), NUM_ITEMS);
    }

    @Test
    public void test_pipelineApi_filterPartitioned() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(journaledMap, alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
         .withoutTimestamps()
         .groupingKey(i -> i % 10)
         .filterUsingContextAsync(contextFactory,
                 transformPartitionedFn(i -> i % 2 == 0))
         .setLocalParallelism(2)
         .drainTo(Sinks.list(sinkList));

        inst.newJob(p, jobConfig);
        assertResult(i -> i % 2 == 0 ? Stream.of(i + "") : Stream.empty(), NUM_ITEMS);
    }

    private <R> DistributedBiFunction<ExecutorService, Integer, CompletableFuture<R>> transformNotPartitionedFn(
            DistributedFunction<Integer, R> transformFn
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

    private <R> DistributedTriFunction<ExecutorService, Integer, Integer, CompletableFuture<R>> transformPartitionedFn(
            DistributedFunction<Integer, R> transformFn
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

    private void assertResult(Function<Integer, Stream<? extends String>> transformFn, int numItems) {
        String expected = IntStream.range(0, numItems)
                                         .boxed()
                                         .flatMap(transformFn)
                                         .sorted()
                                         .collect(joining("\n"));
        assertTrueEventually(() -> assertEquals(expected, sinkList.stream().map(Object::toString).sorted()
                                                                  .collect(joining("\n"))));
    }
}
