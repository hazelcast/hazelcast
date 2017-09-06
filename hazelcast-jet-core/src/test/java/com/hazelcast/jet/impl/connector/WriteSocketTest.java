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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.processor.SinkProcessors.writeSocket;
import static com.hazelcast.jet.processor.SourceProcessors.readMap;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class WriteSocketTest extends JetTestSupport {

    private static final int ITEM_COUNT = 1000;

    @Test
    public void unitTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        new Thread(() -> uncheckRun(() -> {
            Socket socket = serverSocket.accept();
            serverSocket.close();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (reader.readLine() != null) {
                    counter.incrementAndGet();
                }
            }
        })).start();

        ArrayDequeInbox inbox = new ArrayDequeInbox();
        range(0, ITEM_COUNT).forEach(inbox::add);

        Processor p = writeSocket("localhost", serverSocket.getLocalPort()).get(1).iterator().next();
        p.init(mock(Outbox.class), new ProcCtx(null, null, null, 0));
        p.process(0, inbox);
        p.complete();
        assertTrueEventually(() -> assertTrue(counter.get() >= ITEM_COUNT));
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(ITEM_COUNT, counter.get());
    }

    @Test
    public void integrationTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        new Thread(() -> uncheckRun(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                new Thread(() -> uncheckRun(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        while (reader.readLine() != null) {
                            counter.incrementAndGet();
                        }
                    }
                })).start();
            }
        })).start();

        JetInstance jetInstance = createJetMember();
        createJetMember();
        IStreamMap<Integer, String> map = jetInstance.getMap("map");
        range(0, ITEM_COUNT).forEach(i -> map.put(i, String.valueOf(i)));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap("map"));
        Vertex sink = dag.newVertex("sink", writeSocket("localhost", serverSocket.getLocalPort()));

        dag.edge(between(source, sink));

        jetInstance.newJob(dag).join();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        serverSocket.close();
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(ITEM_COUNT, counter.get());
    }

}
