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

package com.hazelcast.jet.pipeline;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.Util.RunnableExc;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.Collections.newSetFromMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class StreamSourceStageTestBase extends JetTestSupport {

    protected static JetInstance[] instances;
    static final String JOURNALED_MAP_NAME = "journaledMap";
    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withoutTimestampsFn =
            s -> s.withoutTimestamps().addTimestamps(i -> i + 1, 0).setLocalParallelism(1);
    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withNativeTimestampsFn =
            s -> s.withNativeTimestamps(0);
    protected final Function<StreamSourceStage<Integer>, StreamStage<Integer>> withTimestampsFn =
            s -> s.withTimestamps(i -> i + 1, 0).setLocalParallelism(1);

    @Before
    public void before() {
        WatermarkLogger.watermarks.clear();
    }

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName(JOURNALED_MAP_NAME)
                .setEnabled(true));
        // use 1 partition for the map journal to have an item in each ption
        config.getHazelcastConfig().setProperty(PARTITION_COUNT.getName(), "1");
        instances = factory.newMembers(config, 2);
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
        StreamSourceStage<Integer> sourceStage = p.drawFrom(source);
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
                .window(WindowDefinition.tumbling(1))
                .aggregate(counting())
                .peek()
                .drainTo(Sinks.fromProcessor("s", ProcessorMetaSupplier.of(WatermarkLogger::new)));
        Job job = instances[0].newJob(p);

        HashSet<Long> expectedWmsSet = new HashSet<>(expectedWms);

        RunnableExc assertTask = () -> assertEquals(expectedWmsSet, WatermarkLogger.watermarks);
        assertTrueEventually(assertTask, 24);
        assertTrueAllTheTime(assertTask, 1);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));
        job.cancel();
        expectedException.expect(CancellationException.class);
        job.join();
    }

    private static class WatermarkLogger extends AbstractProcessor {
        private static Set<Long> watermarks = newSetFromMap(new ConcurrentHashMap<>());

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
