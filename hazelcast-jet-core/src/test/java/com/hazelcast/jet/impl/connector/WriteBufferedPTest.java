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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastSerialClassRunner.class)
public class WriteBufferedPTest extends JetTestSupport {

    private static final List<String> events = new CopyOnWriteArrayList<>();

    @Before
    public void setup() {
        events.clear();
    }

    @Test
    public void writeBuffered_smokeTest() throws Exception {
        Processor p = getLoggingBufferedWriter().get(1).iterator().next();
        Outbox outbox = mock(Outbox.class);
        p.init(outbox, mock(Context.class));
        TestInbox inbox = new TestInbox();
        inbox.add(1);
        inbox.add(2);
        p.process(0, inbox);
        inbox.add(3);
        inbox.add(4);
        p.process(0, inbox);
        p.tryProcessWatermark(new Watermark(0)); // watermark should not be written
        p.process(0, inbox); // empty flush
        p.complete();

        assertEquals(asList(
                "new",
                "add:1",
                "add:2",
                "flush",
                "add:3",
                "add:4",
                "flush",
                "flush",
                "dispose"
        ), events);

        assertEquals(0, inbox.size());
        verifyZeroInteractions(outbox);
    }

    @Test
    public void when_writeBufferedJobFailed_then_bufferDisposed() throws Exception {
        JetInstance instance = createJetMember();
        try {
            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", StuckForeverSourceP::new);
            Vertex sink = dag.newVertex("sink", getLoggingBufferedWriter()).localParallelism(1);

            dag.edge(Edge.between(source, sink));

            Job job = instance.newJob(dag);
            // wait for the job to initialize
            Thread.sleep(5000);
            job.cancel();

            assertTrueEventually(() -> assertTrue("No \"dispose\", only: " + events, events.contains("dispose")), 60);
            System.out.println(events);
        } finally {
            instance.shutdown();
        }
    }

    // returns a processor that will not write anywhere, just log the events
    private static ProcessorSupplier getLoggingBufferedWriter() {
        return SinkProcessors.writeBufferedP(
                idx -> {
                    events.add("new");
                    return null;
                },
                (buffer, item) -> events.add("add:" + item),
                buffer -> events.add("flush"),
                buffer -> events.add("dispose")
        );
    }
}
