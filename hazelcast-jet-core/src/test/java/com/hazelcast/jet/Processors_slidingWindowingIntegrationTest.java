/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.WatermarkPolicies.limitingLagAndLull;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.processor.Processors.accumulateByFrame;
import static com.hazelcast.jet.processor.Processors.aggregateToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.insertWatermarks;
import static com.hazelcast.jet.processor.SinkProcessors.writeList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class Processors_slidingWindowingIntegrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStageProcessor;

    @Parameter(1)
    public boolean isBatch;

    @Parameters(name = "singleStageProcessor={0}, isBatch={1}")
    public static Collection<Object> parameters() {
        return asList(
                new Object[]{true, true},
                new Object[]{true, false},
                new Object[]{false, true},
                new Object[]{false, false}
        );
    }

    @After
    public void shutdownAll() {
        shutdownFactory();
    }

    @Test
    public void smokeTest() throws Exception {
        runTest(
                singletonList(new MyEvent(10, "a", 1L)),
                asList(
                        new TimestampedEntry<>(1000, "a", 1L),
                        new TimestampedEntry<>(2000, "a", 1L)
                ));
    }

    private void runTest(List<MyEvent> sourceEvents, List<TimestampedEntry<String, Long>> expectedOutput)
            throws Exception {
        JetInstance instance = createJetMember();

        WindowDefinition wDef = slidingWindowDef(2000, 1000);
        AggregateOperation1<Object, ?, Long> counting = counting();

        DAG dag = new DAG();
        boolean isBatchLocal = isBatch; // to prevent serialization of whole class
        Vertex source = dag.newVertex("source", () -> new EmitListP(sourceEvents, isBatchLocal)).localParallelism(1);
        Vertex insertPP = dag.newVertex("insertWmP", insertWatermarks(MyEvent::getTimestamp,
                limitingLagAndLull(500, 1000), emitByFrame(wDef))).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList("sink"));

        dag.edge(between(source, insertPP).isolated());

        if (singleStageProcessor) {
            Vertex slidingWin = dag.newVertex("slidingWin",
                    aggregateToSlidingWindow(MyEvent::getKey,
                            MyEvent::getTimestamp, TimestampKind.EVENT, wDef, counting));
            dag
                    .edge(between(insertPP, slidingWin).partitioned(MyEvent::getKey).distributed())
                    .edge(between(slidingWin, sink).isolated());

        } else {
            Vertex accumulateByFrame = dag.newVertex("accumulateByFrame",
                    accumulateByFrame(MyEvent::getKey,
                            MyEvent::getTimestamp, TimestampKind.EVENT, wDef, counting));
            Vertex slidingWin = dag.newVertex("slidingWin", combineToSlidingWindow(wDef, counting));
            dag
                    .edge(between(insertPP, accumulateByFrame).partitioned(MyEvent::getKey))
                    .edge(between(accumulateByFrame, slidingWin).partitioned(entryKey()).distributed())
                    .edge(between(slidingWin, sink).isolated());
        }

        Job job = instance.newJob(dag);

        if (isBatch) {
            job.join();
        }

        IList<MyEvent> sinkList = instance.getList("sink");

        assertTrueEventually(() ->
                assertEquals(streamToString(expectedOutput.stream()), streamToString(new ArrayList<>(sinkList).stream())),
                5);
        // wait a little more and make sure, that there are no more frames
        Thread.sleep(1000);

        assertTrue(sinkList.size() == expectedOutput.size());
    }

    private static String streamToString(Stream<?> stream) {
        return stream
                .map(String::valueOf)
                .collect(Collectors.joining("\n"));
    }

    /**
     * A processor that will emit contents of a list and optionally complete.
     */
    private static class EmitListP extends AbstractProcessor {
        private final List<?> list;
        private final boolean complete;

        EmitListP(List<?> list, boolean complete) {
            this.list = list;
            this.complete = complete;
        }

        @Override
        public boolean complete() {
            for (Object o : list) {
                emit(o);
            }
            if (!complete) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // proceed to returning true
                }
            }
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }

    private static class MyEvent extends SimpleImmutableEntry<String, Long> {
        private final long timestamp;

        MyEvent(long timestamp, String key, Long value) {
            super(key, value);
            this.timestamp = timestamp;
        }

        long getTimestamp() {
            return timestamp;
        }
    }
}
