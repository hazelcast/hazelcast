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

package com.hazelcast.jet.windowing;

import com.hazelcast.core.IList;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Processors.NoopP;
import com.hazelcast.jet.Vertex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndLull;
import static com.hazelcast.jet.windowing.StreamingTestSupport.streamToString;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowIntegrationTest extends JetTestSupport {

    @After
    public void shutdownAll() {
        shutdownFactory();
    }

    @Test
    public void test() throws InterruptedException {
        runTest(
                Collections.singletonList(new MockEvent("a", 10, 1)),
                Arrays.asList(
                        new Frame(1000, "a", 1L),
                        new Frame(2000, "a", 1L)
                ));
    }

    public void runTest(List<MockEvent> sourceEvents, List<Frame> expectedOutput) throws InterruptedException {
        JetInstance instance = super.createJetMember();

        WindowDefinition wDef = slidingWindowDef(2000, 1000);
        WindowOperation<Object, ?, Long> wOperation = WindowOperations.counting();

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamList(sourceEvents)).localParallelism(1);
        Vertex insertPP = dag.newVertex("insertPP", insertPunctuation(MockEvent::getEventSeq,
                () -> cappingEventSeqLagAndLull(500, 1000).throttleByFrame(wDef)))
                .localParallelism(1);
        Vertex gbfp = dag.newVertex("gbfp", groupByFrame(MockEvent::getKey, MockEvent::getEventSeq, wDef, wOperation));
        Vertex sliwp = dag.newVertex("sliwp", slidingWindow(wDef, wOperation));
        Vertex sink = dag.newVertex("sink", writeList("sink"));

        dag
                .edge(between(source, insertPP).oneToMany())
                .edge(between(insertPP, gbfp).partitioned(MockEvent::getKey))
                .edge(between(gbfp, sliwp).partitioned((Function<Frame, Object>) Frame::getKey).distributed())
                .edge(between(sliwp, sink).oneToMany());

        instance.newJob(dag).execute();

        IList<Frame> sinkList = instance.getList("sink");

        assertTrueEventually(() -> assertTrue(sinkList.size() == expectedOutput.size()));
        // wait little more and make sure, that there are no more frames
        Thread.sleep(2000);

        String expected = streamToString(expectedOutput.stream());
        String actual = streamToString(new ArrayList<>(sinkList).stream());
        assertEquals(expected, actual);
    }

    /**
     * Returns a {@link ProcessorMetaSupplier}, that will emit contents of a list and the NOT complete.
     * Emits from single node and single processor instance.
     */
    private ProcessorMetaSupplier streamList(List<?> sourceList) {
        return addresses -> address -> count ->
                IntStream.range(0, count)
                        .mapToObj(i -> i == 0 ? new StreamListP(sourceList) : new NoopP())
                        .collect(toList());
    }

    private static class MockEvent implements Serializable {
        private final String key;
        private final long eventSeq;
        private final long value;

        private MockEvent(String key, long eventSeq, long value) {
            this.key = key;
            this.eventSeq = eventSeq;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public long getEventSeq() {
            return eventSeq;
        }

        public long getValue() {
            return value;
        }
    }

    private class StreamListP extends AbstractProcessor {
        private final List<?> list;

        public StreamListP(List<?> list) {
            this.list = list;
        }

        @Override
        public boolean complete() {
            for (Object o : list) {
                emit(o);
            }
            // sleep forever
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException ignored) {
            }
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }
}
