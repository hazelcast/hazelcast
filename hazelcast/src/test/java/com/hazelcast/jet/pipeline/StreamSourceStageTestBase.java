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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.test.AssertTask;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class StreamSourceStageTestBase extends JetTestSupport {

    protected static HazelcastInstance instance;
    static final String JOURNALED_MAP_NAME = "journaledMap";
    private static TestHazelcastFactory factory = new TestHazelcastFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // we must use local parallelism 1 on the stages to avoid wm coalescing
    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withoutTimestampsFn =
            s -> s.withoutTimestamps().setLocalParallelism(1).addTimestamps(i -> i + 1, 0);
    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withNativeTimestampsFn =
            s -> s.withNativeTimestamps(0).setLocalParallelism(1);
    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withTimestampsFn =
            s -> s.withTimestamps(i -> i + 1, 0).setLocalParallelism(1);

    @Before
    public void before() {
        WatermarkCollector.watermarks.clear();
    }

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.getMapConfig("*")
              .getEventJournalConfig().setEnabled(true);
        config.getJetConfig().setEnabled(true);
        // use 1 partition for the map journal to have an item in each ption
        config.setProperty(PARTITION_COUNT.getName(), "1");
        instance = factory.newHazelcastInstance(config);
    }

    @AfterClass
    public static void afterClass() {
        factory.terminateAll();
    }

    protected void test(
            StreamSource<? extends Integer> source,
            Function<StreamSourceStage<Integer>, StreamStage<Integer>> addTimestampsFunction,
            List<Long> expectedWms,
            String expectedTimestampFailure
    ) {
        Pipeline p = Pipeline.create();
        StreamSourceStage<Integer> sourceStage = p.readFrom(source);
        StreamStage<Integer> stageWithTimestamps;
        try {
            stageWithTimestamps = addTimestampsFunction.apply(sourceStage);
        } catch (Exception e) {
            if (expectedTimestampFailure == null) {
                fail("Unexpected exception: " + e);
            }
            assertEquals(expectedTimestampFailure, e.getMessage());
            return;
        }
        stageWithTimestamps
                .peek()
                // we use a tumbling window of size 1 to force maximum watermark gap to be 1
                // this should be removed once https://github.com/hazelcast/hazelcast-jet/issues/1365
                // fixed. local parallelism should be 1 for the sources to make sure
                // watermarks don't get coalesced
                .window(WindowDefinition.tumbling(1))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.fromProcessor("wmCollector",
                        preferLocalParallelismOne(WatermarkCollector::new))
                );
        Job job = instance.getJet().newJob(p);

        AssertTask assertTask = () -> assertEquals(expectedWms, WatermarkCollector.watermarks);
        assertTrueEventually(assertTask, 24);
        assertTrueAllTheTime(assertTask, 1);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));
        job.cancel();
        expectedException.expect(CancellationException.class);
        job.join();
    }

    private static class WatermarkCollector extends AbstractProcessor {
        static List<Long> watermarks = new CopyOnWriteArrayList<>();

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return true;
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            watermarks.add(watermark.timestamp());
            return true;
        }
    }
}
