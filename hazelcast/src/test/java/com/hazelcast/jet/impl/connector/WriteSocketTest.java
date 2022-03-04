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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeSocketP;
import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteSocketTest extends JetTestSupport {

    private static final int ITEM_COUNT = 1000;

    @Test
    public void unitTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        Runnable acceptConnection = () -> spawn(() -> uncheckRun(() -> {
            Socket socket = serverSocket.accept();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (reader.readLine() != null) {
                    counter.incrementAndGet();
                }
            }
        }));

        verifyProcessor(writeSocketP("localhost", serverSocket.getLocalPort(), Object::toString, UTF_8))
                .input(range(0, ITEM_COUNT).boxed().collect(toList()))
                .executeBeforeEachRun(acceptConnection)
                .disableSnapshots()
                .expectOutput(emptyList());

        int expectedReceivedItems = ITEM_COUNT * 2; // TestSupport executed each test twice
        assertEqualsEventually(expectedReceivedItems, counter);
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(expectedReceivedItems, counter.get());

        serverSocket.close();
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

        HazelcastInstance hz = createHazelcastInstance();
        createHazelcastInstance();
        IMap<Integer, String> map = hz.getMap("map");
        range(0, ITEM_COUNT).forEach(i -> map.put(i, String.valueOf(i)));

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map("map"))
         .writeTo(Sinks.socket("localhost", serverSocket.getLocalPort()));

        hz.getJet().newJob(p).join();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        serverSocket.close();
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(ITEM_COUNT, counter.get());
    }

}
