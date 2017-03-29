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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * A reader that connects to a specified socket and reads and emits text line by line.
 * This processor expects a server-side socket to be available for connecting to
 * <p>
 * Each processor instance will create a socket connection to the configured [host:port]
 * which will terminate when the socket is closed by the server.
 */
public class ReadSocketTextStreamP extends AbstractProcessor implements Closeable {

    private final String host;
    private final int port;
    private BufferedReader bufferedReader;

    ReadSocketTextStreamP(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates a supplier for {@link ReadSocketTextStreamP}
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     */
    public static ProcessorSupplier supplier(String host, int port) {
        return new Supplier(host, port);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        getLogger().info("Connecting to socket " + hostAndPort());
        Socket socket = new Socket(host, port);
        getLogger().info("Connected to socket " + hostAndPort());
        bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
    }

    @Override
    public boolean complete() {
        return uncheckCall(this::tryComplete);
    }

    private boolean tryComplete() throws IOException {
        String line = bufferedReader.readLine();
        if (line == null) {
            return true;
        }
        emit(line);
        return false;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            getLogger().info("Closing socket " + hostAndPort());
            bufferedReader.close();
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String host;
        private final int port;
        private transient List<Processor> processors;

        Supplier(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            processors = range(0, count)
                    .mapToObj(i -> new ReadSocketTextStreamP(host, port))
                    .collect(toList());
            return processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(p -> uncheckRun(((ReadSocketTextStreamP) p)::close));
        }
    }

}
