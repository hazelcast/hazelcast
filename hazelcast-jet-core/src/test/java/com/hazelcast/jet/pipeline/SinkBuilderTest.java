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

package com.hazelcast.jet.pipeline;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SinkBuilderTest extends PipelineTestSupport {

    @Test
    public void fileSink() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);
        String listName = randomName();

        // When
        p.drawFrom(Sources.<Integer>list(srcName))
         .drainTo(buildRandomFileSink(listName));

        execute();

        //then
        assertTrueEventually(() -> {
            List<String> paths = new ArrayList<>(jet().<String>getList(listName));
            long count = paths.stream().map(Paths::get)
                              .flatMap(path -> uncheckCall(() -> Files.list(path)))
                              .flatMap(path -> uncheckCall(() -> Files.readAllLines(path).stream()))
                              .count();
            assertEquals(ITEM_COUNT, count);
        });
    }


    @Test
    public void socketSink() throws IOException {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);

        AtomicInteger counter = new AtomicInteger();
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            spawn(() -> uncheckRun(() -> {
                while (!serverSocket.isClosed()) {
                    Socket socket = serverSocket.accept();
                    spawn(() -> uncheckRun(() -> {
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                            while (reader.readLine() != null) {
                                counter.incrementAndGet();
                            }
                        } finally {
                            socket.close();
                        }
                    }));
                }
            }));

            p.drawFrom(Sources.<Integer>list(srcName))
             .drainTo(buildSocketSink(serverSocket.getLocalPort()));

            // When
            execute();

            //Then
            assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        }
    }

    private Sink<Integer> buildRandomFileSink(String listName) {
        return Sinks.<File, Integer>builder(context ->
                uncheckCall(() -> {
                    File directory = createTempDirectory();
                    File file = new File(directory, randomName());
                    assertTrue(file.createNewFile());
                    context.jetInstance().getList(listName).add(directory.toPath().toString());
                    return file;
                }))
                .onReceiveFn((sink, item) -> uncheckRun(() -> {
                    appendToFile(sink, item.toString());
                })).build();
    }

    private Sink<Integer> buildSocketSink(int localPort) {
        return Sinks.<BufferedWriter, Integer>builder(context -> uncheckCall(() -> getSocketWriter(localPort)))
                .onReceiveFn((s, item) -> uncheckRun(() -> s.append((char) item.intValue()).append('\n')))
                .flushFn(s -> uncheckRun(s::flush))
                .destroyFn(s -> uncheckRun(s::close))
                .build();
    }

    private static BufferedWriter getSocketWriter(int localPort) throws IOException {
        OutputStream outputStream = new Socket("localhost", localPort).getOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, UTF_8);
        return new BufferedWriter(outputStreamWriter);
    }
}
