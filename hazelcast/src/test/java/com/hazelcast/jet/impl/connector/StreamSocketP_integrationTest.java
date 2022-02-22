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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamSocketP_integrationTest extends JetTestSupport {

    private static final String HOST = "localhost";
    private static final int PORT = 8888;

    private HazelcastInstance instance;

    @Before
    public void setupEngine() {
        instance = createHazelcastInstance();
    }

    @Test
    public void when_dataWrittenToSocket_then_dataImmediatelyEmitted() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        // Given
        try (ServerSocket socket = new ServerSocket(PORT)) {
            spawn(() -> uncheckRun(() -> {
                Socket accept1 = socket.accept();
                Socket accept2 = socket.accept();
                PrintWriter writer1 = new PrintWriter(accept1.getOutputStream());
                writer1.write("hello1 \n");
                writer1.flush();
                PrintWriter writer2 = new PrintWriter(accept2.getOutputStream());
                writer2.write("hello2 \n");
                writer2.flush();

                assertOpenEventually(latch);
                writer1.write("world1 \n");
                writer1.write("jet1 \n");
                writer1.flush();
                writer2.write("world2 \n");
                writer2.write("jet2 \n");
                writer2.flush();

                accept1.close();
                accept2.close();
            }));

            DAG dag = new DAG();
            Vertex producer = dag.newVertex("producer", streamSocketP(HOST, PORT, UTF_8)).localParallelism(2);
            Vertex consumer = dag.newVertex("consumer", writeListP("consumer")).localParallelism(1);
            dag.edge(between(producer, consumer));

            // When
            Job job = instance.getJet().newJob(dag);
            IList<Object> list = instance.getList("consumer");

            assertTrueEventually(() -> assertEquals(2, list.size()));
            latch.countDown();
            job.join();
            assertEquals(6, list.size());
        }
    }

    @Test
    public void when_jobCancelled_then_readerClosed() throws Exception {
        try (ServerSocket socket = new ServerSocket(PORT)) {
            AtomicReference<Socket> accept = new AtomicReference<>();
            CountDownLatch acceptationLatch = new CountDownLatch(1);
            // Cancellation only works, if there are data on socket. Without data, SocketInputStream.read()
            // blocks indefinitely. The StreamSocketP should be improved to use NIO.
            spawn(() -> uncheckRun(() -> {
                accept.set(socket.accept());
                acceptationLatch.countDown();
                byte[] word = "jet\n".getBytes();
                try (OutputStream outputStream = accept.get().getOutputStream()) {
                    while (true) {
                        outputStream.write(word);
                        outputStream.flush();
                        Thread.sleep(1000);
                    }
                }
            }));

            Vertex producer = new Vertex("producer", streamSocketP(HOST, PORT, UTF_8)).localParallelism(1);
            Vertex sink = new Vertex("sink", noopP()).localParallelism(1);
            DAG dag = new DAG()
                    .vertex(producer)
                    .vertex(sink)
                    .edge(between(producer, sink));

            Job job = instance.getJet().newJob(dag);
            acceptationLatch.await();
            job.cancel();

            assertTrueEventually(() -> assertTrue("Socket not closed", accept.get().isClosed()));
        }
    }
}
