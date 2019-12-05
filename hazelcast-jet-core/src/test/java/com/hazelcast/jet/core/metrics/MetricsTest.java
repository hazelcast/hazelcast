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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class MetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
    }

    @Test
    public void counter_notUsed() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Metrics.metric("dropped"); //retrieve "dropped" counter, but never use it
                    }
                    //not even retrieve "total" counter

                    return pass;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "dropped", 0, "total", null);
    }

    @Test
    public void counter() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Metrics.metric("dropped").increment();
                    }
                    Metrics.metric("total").increment();
                    Metrics.metric("last").set(l);

                    return pass;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "dropped", 2, "total", 5);
    }

    @Test
    public void gauge() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metric metric = Metrics.metric("sum", Unit.COUNT);
                    metric.set(acc.get());
                    return acc.get();
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "sum", 10);
    }

    @Test
    public void gauge_notUsed() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metrics.metric("sum", Unit.COUNT);
                    return acc.get();
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "sum", 0L);
    }

    @Test
    public void nonCooperativeProcessor() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source", TestProcessors.ListSource.supplier(asList(1L, 2L, 3L)));
        Vertex map = dag.newVertex("map", new NonCoopTransformPSupplier((FunctionEx<Long, Long>) l -> {
            Metrics.metric("mapped").increment();
            return l * 10L;
        }));
        Vertex sink = dag.newVertex("sink", writeListP("results"));

        dag.edge(between(source, map)).edge(between(map, sink));

        Job job = runPipeline(dag);
        assertMetricsProduced(job, "mapped", 3L);
        assertEquals(
                new HashSet<>(Arrays.asList(10L, 20L, 30L)),
                new HashSet<>(instance.getList("results"))
        );
    }

    @Test
    public void metricsDisabled() {
        Long[] input = {0L, 1L, 2L, 3L, 4L};
        pipeline.readFrom(TestSources.items(input))
                .map(l -> {
                    Metrics.metric("mapped").increment();
                    Metrics.metric("total", Unit.COUNT).set(input.length);
                    return l;
                })
                .writeTo(Sinks.logger());

        Job job = instance.newJob(pipeline, new JobConfig().setMetricsEnabled(false));
        job.join();

        JobMetrics metrics = job.getMetrics();
        assertTrue(metrics.get("mapped").isEmpty());
        assertTrue(metrics.get("total").isEmpty());
    }

    @Test
    public void usingContextAsync() {
        int inputSize = 100_000;

        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .addTimestamps(i -> i, 0L)
                .filterUsingServiceAsync(
                        ServiceFactory.withCreateFn(i -> 0L),
                        (ctx, l) -> {
                            Metric dropped = Metrics.threadSafeMetric("dropped");
                            Metric total = Metrics.threadSafeMetric("total");
                            return CompletableFuture.supplyAsync(
                                    () -> {
                                        boolean pass = l % 2L == ctx;
                                        if (!pass) {
                                            dropped.increment();
                                        }
                                        total.increment();
                                        return pass;
                                    }
                            );
                        }
                )
                .writeTo(Sinks.noop());

        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        assertTrueEventually(() -> assertEquals(inputSize,
                job.getMetrics().get("total").stream().mapToLong(Measurement::value).sum()));
        assertTrueEventually(() -> assertEquals(inputSize / 2,
                job.getMetrics().get("dropped").stream().mapToLong(Measurement::value).sum()));
        job.join();
    }

    @Test
    public void availableDuringJobExecution() {
        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.logger());

        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        assertTrueEventually(() -> {
            assertMetricsProduced(job, "total", generatedItems);
        });
    }

    @Test
    public void when_metricsDisabled_then_unavailableDuringJobExecution() {
        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.list("sink"));

        Job job = instance.newJob(pipeline, new JobConfig().setMetricsEnabled(false));
        List<Object> list = instance.getList("sink");
        assertTrueEventually(() -> {
            assertTrue(!list.isEmpty());
        });
        assertTrue(job.getMetrics().get("total").isEmpty());
    }

    @Test
    public void when_jetMetricNameIsUsed_then_itIsNotOverwritten() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    Metrics.metric("emittedCount").increment(1000);
                    return true;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        // expected value is number_of_emitted_items (5) * number_of_vertex (2) = 10
        assertJetMetricsProduced(job, "emittedCount", 10);
        // expected value is number_of_emitted_items (5) * 1000 + expected_jet_metric_value (10)
        assertMetricsProduced(job, "emittedCount", 10 + 5 * 1000);
    }

    @Test
    public void metricInFusedStages() {
        int inputSize = 100_000;
        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .filter(l -> {
                    Metrics.metric("onlyInFilter").increment();
                    Metrics.metric("inBoth").increment();
                    return true;
                })
                .map(t -> {
                    Metrics.metric("onlyInMap").increment();
                    Metrics.metric("inBoth").increment();
                    return t;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "onlyInFilter", inputSize, "onlyInMap", inputSize, "inBoth", 2 * inputSize);
    }

    @Test
    public void metricInNotFusedStages() {
        int inputSize = 100_000;
        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .filter(l -> {
                    Metrics.metric("onlyInFilter").increment();
                    Metrics.metric("inBoth").increment();
                    return true;
                })
                .groupingKey(t -> t)
                .aggregate(counting())
                .map(t -> {
                    Metrics.metric("onlyInMap").increment();
                    Metrics.metric("inBoth").increment();
                    return t;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "onlyInFilter", inputSize, "onlyInMap", inputSize, "inBoth", 2 * inputSize);
    }

    @Test
    public void metricsAreRestartedIfPipelineIsRunTwice() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    Metrics.metric("total").increment();
                    return true;
                })
                .writeTo(Sinks.logger());

        Job job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "total", 5);
        job = runPipeline(pipeline.toDag());
        assertMetricsProduced(job, "total", 5);
    }

    @Test
    public void metricsAreJobIndependent() {
        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < 4000)
                .map(t -> {
                    if (t.sequence() % 4 == 0) {
                        Metrics.metric("total").increment();
                    }
                    return t;
                })
                .writeTo(Sinks.logger());

        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < 4000)
                .map(t -> {
                    if (t.sequence() % 4 != 0) {
                        Metrics.metric("total").increment();
                    }
                    return t;
                })
                .writeTo(Sinks.logger());

        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        Job job2 = instance.newJob(pipeline2, JOB_CONFIG_WITH_METRICS);
        assertTrueEventually(() -> {
            assertMetricsProduced(job, "total", 1000);
        });
        assertTrueEventually(() -> {
            assertMetricsProduced(job2, "total", 3000);
        });
    }

    @Test
    public void availableViaJmx() throws Exception {
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.logger());

        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        assertTrueEventually(() -> {
            assertMetricsProduced(job, "total", generatedItems);
        });

        List<String> mBeanNames = new ArrayList<>();
        String memberName = instance.getHazelcastInstance().getName();
        String jobId = job.getIdString();
        String execId = job.getMetrics().get("total").get(0).tag("exec");
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < availableProcessors; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("com.hazelcast.jet:type=Metrics,instance=")
                    .append(memberName)
                    .append(",tag0=\"job=")
                    .append(jobId)
                    .append("\",tag1=\"exec=")
                    .append(execId)
                    .append("\",tag2=\"vertex=fused(filter, map)\",tag3=\"procType=TransformP\",tag4=\"proc=")
                    .append(i)
                    .append("\",tag5=\"user=true\"");
            mBeanNames.add(sb.toString());
        }

        long sum = 0;
        for (String mBeanName : mBeanNames) {
            ObjectName objectName = new ObjectName(mBeanName);
            long attributeValue = (long) platformMBeanServer.getAttribute(objectName, "total");
            sum += attributeValue;
        }
        assertEquals(generatedItems, sum);
    }

    private Job runPipeline(DAG dag) {
        Job job = instance.newJob(dag, JOB_CONFIG_WITH_METRICS);
        job.join();
        return job;
    }

    private void assertMetricsProduced(Job job, Object... expected) {
        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            List<Measurement> measurements = metrics.get(name);
            assertMetricValue(name, measurements, expected[i + 1]);
        }
    }

    private void assertJetMetricsProduced(Job job, Object... expected) {
        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            List<Measurement> measurements = metrics.filter(
                    MeasurementPredicates.tagValueEquals("user", "true").negate()).get(name);
            assertMetricValue(name, measurements, expected[i + 1]);
        }
    }

    private void assertMetricValue(String name, List<Measurement> measurements, Object expected) {
        if (expected == null) {
            assertTrue(
                    String.format("Did not expect measurements for metric '%s', but there were some", name),
                    measurements.isEmpty()
            );
        } else {
            assertFalse(
                    String.format("Expected measurements for metric '%s', but there were none", name),
                    measurements.isEmpty()
            );
            long actualValue = measurements.stream().mapToLong(Measurement::value).sum();
            if (expected instanceof Number) {
                long expectedValue = ((Number) expected).longValue();
                assertEquals(
                        String.format("Expected %d for metric '%s', but got %d instead", expectedValue, name,
                                actualValue),
                        expectedValue,
                        actualValue
                );
            } else {
                long expectedMinValue = ((long[]) expected)[0];
                long expectedMaxValue = ((long[]) expected)[1];
                assertTrue(
                        String.format("Expected a value in the range [%d, %d] for metric '%s', but got %d",
                                expectedMinValue, expectedMaxValue, name, actualValue),
                        expectedMinValue <= actualValue && actualValue <= expectedMaxValue
                );
            }
        }
    }

    private static class NonCoopTransformPSupplier implements SupplierEx<Processor> {

        private final FunctionEx<Long, Long> mappingFn;

        NonCoopTransformPSupplier(FunctionEx<Long, Long> mappingFn) {
            this.mappingFn = mappingFn;
        }

        @Override
        public Processor getEx() {
            final ResettableSingletonTraverser<Long> trav = new ResettableSingletonTraverser<>();
            return new TransformP<Long, Long>(item -> {
                trav.accept(((FunctionEx<? super Long, ? extends Long>) mappingFn).apply(item));
                return trav;
            }) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
        }
    }

}
