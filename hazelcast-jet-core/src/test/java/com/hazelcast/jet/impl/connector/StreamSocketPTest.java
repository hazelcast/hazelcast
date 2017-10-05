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
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class StreamSocketPTest extends JetTestSupport {

    private Queue<Object> bucket;
    private TestOutbox outbox;
    private TestProcessorContext context;

    @Before
    public void before() {
        outbox = new TestOutbox(10);
        context = new TestProcessorContext();
        bucket = outbox.queueWithOrdinal(0);
    }

    @Test
    public void smokeTest() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread thread = new Thread(() -> uncheckRun(() -> {
                Socket socket = serverSocket.accept();
                PrintWriter writer = new PrintWriter(socket.getOutputStream());
                writer.write("hello\n");
                writer.write("world\n");
                writer.close();
                socket.close();
            }));
            thread.start();

            Processor processor = SourceProcessors.streamSocketP("localhost", serverSocket.getLocalPort(), UTF_8)
                                                  .get(1).iterator().next();
            processor.init(outbox, outbox, context);

            assertTrueEventually(() -> assertTrue(processor.complete()), 3);
            assertEquals("hello", bucket.poll());
            assertEquals("world", bucket.poll());
            assertEquals(null, bucket.poll());
            assertTrueEventually(() -> assertFalse(thread.isAlive()));
        }
    }

}
