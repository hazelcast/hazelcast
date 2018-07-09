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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeSocketP;
import static com.hazelcast.jet.core.test.TestSupport.supplierFrom;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
public class WriteSocketTest extends JetTestSupport {

    private static final int ITEM_COUNT = 1000;

    @Test
    public void unitTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        spawn(() -> uncheckRun(() -> {
            Socket socket = serverSocket.accept();
            serverSocket.close();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (reader.readLine() != null) {
                    counter.incrementAndGet();
                }
            }
        }));

        TestInbox inbox = new TestInbox();
        range(0, ITEM_COUNT).forEach(inbox::add);

        Processor p = supplierFrom(writeSocketP("localhost", serverSocket.getLocalPort(), Object::toString, UTF_8))
                .get();
        p.init(mock(Outbox.class), new TestProcessorContext());
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
        spawn(() -> uncheckRun(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                spawn(() -> uncheckRun(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        while (reader.readLine() != null) {
                            counter.incrementAndGet();
                        }
                    }
                }));
            }
        }));

        JetInstance jetInstance = createJetMember();
        createJetMember();
        IMapJet<Integer, String> map = jetInstance.getMap("map");
        range(0, ITEM_COUNT).forEach(i -> map.put(i, String.valueOf(i)));

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map("map"))
         .drainTo(Sinks.socket("localhost", serverSocket.getLocalPort()));

        jetInstance.newJob(p).join();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        serverSocket.close();
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(ITEM_COUNT, counter.get());
    }

}
