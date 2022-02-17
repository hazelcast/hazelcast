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

package com.hazelcast.jet.pipeline;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.SinkBuilder.sinkBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SinkBuilderTest extends PipelineTestSupport {

    @Test
    public void fileSink() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);
        String listName = randomName();
        BatchStage<Integer> stage = p.readFrom(Sources.list(srcName));

        // When
        Sink<Integer> sink = sinkBuilder("file-sink",
                        context -> {
                            File directory = createTempDirectory();
                            File file = new File(directory, randomName());
                            assertTrue(file.createNewFile());
                            context.hazelcastInstance().getList(listName).add(directory.toPath().toString());
                            return file;
                        })
                .receiveFn((File sink1, Integer item) -> appendToFile(sink1, item.toString()))
                .build();

        //Then
        stage.writeTo(sink);
        execute();
        List<String> paths = new ArrayList<>(hz().getList(listName));
        long count = paths.stream().map(Paths::get)
                          .flatMap(path -> uncheckCall(() -> Files.list(path)))
                          .flatMap(path -> uncheckCall(() -> Files.readAllLines(path).stream()))
                          .count();
        assertEquals(itemCount, count);
    }

    @Test
    public void socketSink() throws IOException {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);

        AtomicInteger counter = new AtomicInteger();
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            spawn(() -> uncheckRun(() -> {
                while (!serverSocket.isClosed()) {
                    Socket socket = serverSocket.accept();
                    spawn(() -> uncheckRun(() -> {
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                            while (in.readLine() != null) {
                                counter.incrementAndGet();
                            }
                        } finally {
                            socket.close();
                        }
                    }));
                }
            }));
            BatchStage<Integer> stage = p.readFrom(Sources.list(srcName));

            // When
            int localPort = serverSocket.getLocalPort();
            Sink<Integer> sink = sinkBuilder("socket-sink", jet -> getSocketWriter(localPort))
                    .receiveFn((PrintWriter w, Integer x) -> w.println(x))
                    .flushFn(PrintWriter::flush)
                    .destroyFn(PrintWriter::close)
                    .build();

            //Then
            stage.writeTo(sink);
            execute();
            assertTrueEventually(() -> assertEquals(itemCount, counter.get()));
        }
    }

    private static PrintWriter getSocketWriter(int localPort) throws IOException {
        OutputStream outputStream = new Socket("localhost", localPort).getOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, UTF_8);
        return new PrintWriter(outputStreamWriter);
    }
}
