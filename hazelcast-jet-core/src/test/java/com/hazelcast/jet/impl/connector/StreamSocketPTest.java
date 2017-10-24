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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static com.hazelcast.jet.core.test.TestSupport.supplierFrom;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class StreamSocketPTest extends JetTestSupport {

    @Parameter
    public String input;

    @Parameter(1)
    public List<String> output;

    private Queue<Object> bucket;
    private TestOutbox outbox;
    private TestProcessorContext context;

    @Parameters(name = "input={0}, output={1}")
    public static Collection<Object[]> parameters() {
        List<String> onaAndTwo = asList("1", "2");
        return asList(
                new Object[]{"1\n2\n", onaAndTwo},
                new Object[]{"1\r\n2\r\n", onaAndTwo},
                new Object[]{"1\n2\r\n", onaAndTwo},
                new Object[]{"1\r\n2\n", onaAndTwo},
                new Object[]{"1\r2\n", onaAndTwo}, // mixed line terminators
                new Object[]{"", emptyList()},
                new Object[]{"\n", singletonList("")},
                new Object[]{"1", emptyList()}, // no line terminator after the only line
                new Object[]{"1\n2", singletonList("1")}, // no line terminator after the last line
                new Object[]{"1\n\n2\n", asList("1", "", "2")}
        );
    }

    @Before
    public void before() {
        outbox = new TestOutbox(10);
        context = new TestProcessorContext();
        bucket = outbox.queueWithOrdinal(0);
    }

    @Test
    public void smokeTest() throws Exception {
        // we'll test the input as if it is split at every possible position. This is to test the logic that input can be
        // split at any place: between \r\n, between the bytes of utf-16 sequence etc
        byte[] inputBytes = input.getBytes(UTF_16);
        for (int splitIndex = 0; splitIndex < inputBytes.length; splitIndex++) {
            System.out.println("--------- runTest(" + splitIndex + ") ---------");
            runTest(inputBytes, splitIndex);
        }
    }

    private void runTest(byte[] inputBytes, int inputSplitAfter) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread thread = new Thread(() -> uncheckRun(() -> {
                Socket socket = serverSocket.accept();
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(inputBytes, 0, inputSplitAfter);
                Thread.sleep(300);
                outputStream.write(inputBytes, inputSplitAfter, inputBytes.length - inputSplitAfter);
                outputStream.close();
                socket.close();
            }));
            thread.start();

            Processor processor = supplierFrom(streamSocketP("localhost", serverSocket.getLocalPort(), UTF_16)).get();
            processor.init(outbox, context);

            assertTrueEventually(() -> assertTrue(processor.complete()), 3);
            for (String s : output) {
                assertEquals(s, bucket.poll());
            }

            assertEquals(null, bucket.poll());
            assertTrueEventually(() -> assertFalse(thread.isAlive()));
        }
    }
}
