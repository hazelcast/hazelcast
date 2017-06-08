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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WriteBufferedPTest extends JetTestSupport {

    private List<String> events = new ArrayList<>();

    @Test
    public void writeBuffered_smokeTest() throws Exception {
        Processor p = getLoggingBufferedWriter().get(1).iterator().next();
        Outbox outbox = mock(Outbox.class);
        p.init(outbox, mock(Context.class));
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        inbox.add(1);
        inbox.add(2);
        p.process(0, inbox);
        inbox.add(3);
        inbox.add(4);
        inbox.add(new Punctuation(0)); // punctuation should not be written
        p.process(0, inbox);
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
            Vertex source = dag.newVertex("source", SleepForeverProcessor::new);
            Vertex sink = dag.newVertex("sink", getLoggingBufferedWriter()).localParallelism(1);

            dag.edge(Edge.between(source, sink));

            Future<Void> future = instance.newJob(dag).execute();
            // wait for the job to initialize
            Thread.sleep(500);
            future.cancel(true);

            assertTrueEventually(() -> assertTrue("No \"dispose\", only: " + events, events.contains("dispose")), 60);
            System.out.println(events);
        } finally {
            instance.shutdown();
        }
    }

    private ProcessorSupplier getLoggingBufferedWriter() {
        // returns a processor that will not write anywhere, just log the events instead
        List<String> localEvents = events;
        return Sinks.writeBuffered(
                idx -> {
                    localEvents.add("new");
                    return null;
                },
                (buffer, item) -> localEvents.add("add:" + item),
                buffer -> localEvents.add("flush"),
                buffer -> localEvents.add("dispose")
        );
    }

    private static class SleepForeverProcessor extends AbstractProcessor {
        SleepForeverProcessor() {
            setCooperative(false);
        }

        @Override
        public boolean complete() {
            // sleep forever - we'll cancel the job
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                fail();
            }
            return false;
        }
    }
}
