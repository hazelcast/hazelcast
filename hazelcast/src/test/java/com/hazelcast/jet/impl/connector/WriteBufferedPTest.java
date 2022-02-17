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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBufferedPTest extends JetTestSupport {

    private static final List<String> events = new CopyOnWriteArrayList<>();

    @Before
    public void setup() {
        events.clear();
        TestProcessors.reset(1);
    }

    @Test
    public void writeBuffered_smokeTest() {
        verifyProcessor(getLoggingBufferedWriter())
                .input(asList(1, 2, 3, 4))
                .disableSnapshots()
                .expectOutput(emptyList());

        assertEquals(asList(
                //1st run has inbox limit set to 1 -> it's calling flush after each item
                "new",
                "add:1",
                "flush",
                "add:2",
                "flush",
                "add:3",
                "flush",
                "add:4",
                "flush",
                "dispose",

                //2nd run is with inbox limit set to default
                "new",
                "add:1",
                "add:2",
                "add:3",
                "add:4",
                "flush",
                "dispose"
        ), events);
    }

    @Test
    public void when_writeBufferedJobFailed_then_bufferDisposed() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new NoOutputSourceP());
        Vertex sink = dag.newVertex("sink", getLoggingBufferedWriter()).localParallelism(1);

        dag.edge(Edge.between(source, sink));

        Job job = instance.getJet().newJob(dag);
        // wait for the job to initialize
        Thread.sleep(5000);
        job.cancel();

        assertTrueEventually(() -> assertTrue("No \"dispose\", only: " + events, events.contains("dispose")), 60);
        System.out.println(events);
    }

    // returns a processor that will not write anywhere, just log the events
    private static SupplierEx<Processor> getLoggingBufferedWriter() {
        return SinkProcessors.writeBufferedP(
                idx -> {
                    events.add("new");
                    return "foo";
                },
                (buffer, item) -> events.add("add:" + item),
                buffer -> events.add("flush"),
                buffer -> events.add("dispose")
        );
    }
}
