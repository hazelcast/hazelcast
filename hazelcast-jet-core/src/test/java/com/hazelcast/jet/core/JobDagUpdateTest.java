/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IList;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JetTestSupport.assertJobStatusEventually;
import static com.hazelcast.jet.core.JetTestSupport.assertTrueEventually;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class JobDagUpdateTest {

    private static final String STATE_NAME = "state";

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance instance;
    private Pipeline pipeline1 = Pipeline.create();
    private Pipeline pipeline2 = Pipeline.create();
    private JobConfig jobConfigInitial;
    private JobConfig jobConfigWithRestore;
    private IList<Integer> sink;

    @BeforeClass
    public static void beforeClass() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(1);
        instance = factory.newMember(jetConfig);
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    @Before
    public void before() {
        jobConfigInitial = new JobConfig()
                .setProcessingGuarantee(NONE); // we'll export manually
        jobConfigWithRestore = new JobConfig()
                .setProcessingGuarantee(NONE)
                .setInitialSnapshotName(STATE_NAME);
        sink = instance.getList("sink");
        ProducerP.wasSnapshotted = false;
    }

    @After
    public void after() {
        for (Job job : instance.getJobs()) {
            job.cancel();
            assertTrueEventually(() -> {
                JobStatus status = job.getStatus();
                assertTrue("status=" + status, status == COMPLETED || status == FAILED);
            });
        }
        for (DistributedObject object : instance.getHazelcastInstance().getDistributedObjects()) {
            object.destroy();
        }
    }

    @Test
    public void test_addFilter() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .filter(val -> val % 2 == 0)
                .drainTo(Sinks.list("sink"));

        runPipelines(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18));
    }

    @Test
    public void test_removeFilter() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .filter(val -> val % 2 == 0)
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink"));

        runPipelines(asList(0, 2, 4, 6, 8), asList(0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    }

    @Test
    public void test_changeMapping() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .map(val -> val - 10)
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .map(val -> val + 10)
                .drainTo(Sinks.list("sink"));

        runPipelines(asList(-10, -9, -8, -7, -6, -5, -4, -3, -2, -1),
                asList(-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
    }

    @Test
    public void test_changeSource() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source1", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink"));

        runPipelines(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    public void test_changeSink() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .drainTo(Sinks.list("sink1"));

        runPipelines(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        IListJet<Object> sink1 = instance.getList("sink1");
        assertTrueEventually(() -> assertEquals(asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), new ArrayList<>(sink1)));
    }

    @Test
    public void test_extendSlidingWindowSize_keepSlideBy() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.sliding(2, 1))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.sliding(3, 1))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));

        runPipelines(
                asList(
                        new TimestampedItem(1, 1L),
                        new TimestampedItem(2, 2L),
                        new TimestampedItem(3, 2L),
                        new TimestampedItem(4, 2L),
                        new TimestampedItem(5, 2L),
                        new TimestampedItem(6, 2L),
                        new TimestampedItem(7, 2L),
                        new TimestampedItem(8, 2L),
                        new TimestampedItem(9, 2L)
                ),
                asList(
                        new TimestampedItem(1, 1L),
                        new TimestampedItem(2, 2L),
                        new TimestampedItem(3, 2L),
                        new TimestampedItem(4, 2L),
                        new TimestampedItem(5, 2L),
                        new TimestampedItem(6, 2L),
                        new TimestampedItem(7, 2L),
                        new TimestampedItem(8, 2L),
                        new TimestampedItem(9, 2L),
                        new TimestampedItem(10, 2L),
                        new TimestampedItem(11, 3L),
                        new TimestampedItem(12, 3L),
                        new TimestampedItem(13, 3L),
                        new TimestampedItem(14, 3L),
                        new TimestampedItem(15, 3L),
                        new TimestampedItem(16, 3L),
                        new TimestampedItem(17, 3L),
                        new TimestampedItem(18, 3L),
                        new TimestampedItem(19, 3L)
                ));
    }

    @Test
    public void test_shrinkSlidingWindowSize_keepSlideBy() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.sliding(3, 1))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.sliding(2, 1))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));

        runPipelines(
                asList(
                        new TimestampedItem(1, 1L),
                        new TimestampedItem(2, 2L),
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(4, 3L),
                        new TimestampedItem(5, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(7, 3L),
                        new TimestampedItem(8, 3L),
                        new TimestampedItem(9, 3L)
                ),
                asList(
                        new TimestampedItem(1, 1L),
                        new TimestampedItem(2, 2L),
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(4, 3L),
                        new TimestampedItem(5, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(7, 3L),
                        new TimestampedItem(8, 3L),
                        new TimestampedItem(9, 3L),
                        new TimestampedItem(10, 2L),
                        new TimestampedItem(11, 2L),
                        new TimestampedItem(12, 2L),
                        new TimestampedItem(13, 2L),
                        new TimestampedItem(14, 2L),
                        new TimestampedItem(15, 2L),
                        new TimestampedItem(16, 2L),
                        new TimestampedItem(17, 2L),
                        new TimestampedItem(18, 2L),
                        new TimestampedItem(19, 2L)
                ));
    }

    @Test
    public void test_changeSlidingWindowAggregation_compatible() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(3))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong(i -> i))
                .peek()
                .drainTo(Sinks.list("sink"));

        runPipelines(
                asList(
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(9, 3L)
                ),
                asList(
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(9, 3L),
                        new TimestampedItem(12, 1 + 10 + 11L),
                        new TimestampedItem(15, 12 + 13 + 14L),
                        new TimestampedItem(18, 15 + 16 + 17L)
                ));
    }

    @Test
    public void test_changeAggregation_incompatible_then_fail() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(3))
                .aggregate(counting())
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(2))
                .aggregate(averagingLong(i -> i))
                .drainTo(Sinks.list("sink"));

        Job job1 = instance.newJob(pipeline1, jobConfigInitial);
        assertTrueEventually(() -> assertEquals(asList(
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(9, 3L)
                ), new ArrayList<>(sink)), 10);
        job1.cancelAndExportSnapshot(STATE_NAME);
        ProducerP.wasSnapshotted = false;

        Job job = instance.newJob(pipeline2, jobConfigWithRestore);
        assertJobStatusEventually(job, FAILED);
    }

    @Test
    public void test_changeTwoStageToSingleStage() {
        // We force single stage by using an aggregate operation without a combine fn.
        // Using the AbstractTransform.optimization is currently not available through public api
        AggregateOperation1<Integer, LongAccumulator, Long> countingWithoutCombineFn =
                AggregateOperation.withCreate(LongAccumulator::new)
                                  .andAccumulate((LongAccumulator a, Integer item) -> a.add(1))
                                  .andExportFinish(LongAccumulator::get);
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(3))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.tumbling(3))
                .aggregate(countingWithoutCombineFn)
                .peek()
                .drainTo(Sinks.list("sink"));

        runPipelines(
                asList(
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(9, 3L)
                ),
                asList(
                        new TimestampedItem(3, 3L),
                        new TimestampedItem(6, 3L),
                        new TimestampedItem(9, 3L),
                        new TimestampedItem(12, 3L),
                        new TimestampedItem(15, 3L),
                        new TimestampedItem(18, 3L)
                ));
    }

    @Test
    public void test_changeSessionWindowTimeout() {
        pipeline1
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .filter(i -> i % 4 < 2) // will pass 0, 1, 4, 5, 8, 9, 12, 13, 16, 17
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.session(2))
                .aggregate(summingLong(i -> i))
                .drainTo(Sinks.list("sink"));
        pipeline2
                .drawFrom(Sources.<Integer>streamFromProcessor("source", ProcessorMetaSupplier.of(ProducerP::new)))
                .filter(i -> i % 4 < 2)
                .addTimestamps(i -> i, 0)
                .window(WindowDefinition.session(3))
                .aggregate(summingLong(i -> i))
                .drainTo(Sinks.list("sink"));

        runPipelines(
                asList(
                        new TimestampedItem(3, 1L),
                        new TimestampedItem(7, 9L)
                ),
                asList(
                        new TimestampedItem(3, 1L),
                        new TimestampedItem(7, 4 + 5L),
                        new TimestampedItem(11, 8 + 9L),
                        new TimestampedItem(16, 12 + 13L)
                ));
    }

    /**
     * Run the pipeline1. After the sink contains expected1, cancel the job
     * with snapshot and run pipeline2. Check, that sink eventually contains
     * expected2.
     */
    private <T> void runPipelines(List<T> expected1, List<T> expected2) {
        Job job = instance.newJob(pipeline1, jobConfigInitial);
        assertTrueEventually(() -> assertEquals(expected1, new ArrayList<>(sink)), 10);
        job.cancelAndExportSnapshot(STATE_NAME);
        ProducerP.wasSnapshotted = false;

        instance.newJob(pipeline2, jobConfigWithRestore);
        assertTrueEventually(() -> assertEquals(expected2, new ArrayList<>(sink)), 10);
        sleepSeconds(1);
    }

    /**
     * A streaming processor that will produce integers 0..9 before the first
     * snapshot and 10..19 after the first snapshot or after restore.
     *
     * It should be used with totalParallelism==1
     */
    private static class ProducerP extends AbstractProcessor {
        private static volatile boolean wasSnapshotted;

        private Traverser<Integer> traverser1 = traverseStream(IntStream.range(0, 10).boxed());
        private Traverser<Integer> traverser2 = traverseStream(IntStream.range(10, 20).boxed());

        @Override
        public boolean complete() {
            emitFromTraverser(wasSnapshotted ? traverser2 : traverser1);
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            if (tryEmitToSnapshot("key", "value")) {
                assertFalse("already snapshotted", wasSnapshotted);
                wasSnapshotted = true;
                return true;
            }
            return false;
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            assertEquals("key", key);
            assertEquals("value", value);
            assertFalse("restored after snapshot was made", wasSnapshotted);
            wasSnapshotted = true;
        }

        @Override
        public boolean finishSnapshotRestore() {
//            assertTrue("restoreFromSnapshot wasn't called", wasSnapshotted);
            return true;
        }
    }
}
