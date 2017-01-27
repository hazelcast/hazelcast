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
import static java.util.Collections.singletonList;

/**
 * A reader that connects to a specified socket and reads and emits text line by line.
 * This processor is mainly aimed for testing purposes as it will only support input
 * from a single socket, and expects a server-side socket to be available for connecting
 * to.
 * <p>
 * It will terminate when the socket is closed by the server.
 */
public class SocketTextStreamReader extends AbstractProcessor implements Closeable {

    private final String host;
    private final int port;
    private BufferedReader bufferedReader;
    private Socket socket;

    SocketTextStreamReader(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        getLogger().info("Connecting to socket " + hostAndPort());
        socket = new Socket(host, port);
        getLogger().info("Connected to socket " + hostAndPort());
        bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
    }

    @Override
    public boolean complete() {
        return uncheckCall(this::tryComplete);
    }

    private boolean tryComplete() throws IOException {
        for (String inputLine; (inputLine = bufferedReader.readLine()) != null; ) {
            emit(inputLine);
            if (getOutbox().isHighWater()) {
                return false;
            }
        }
        return true;
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

    /**
     * Creates a supplier for {@link SocketTextStreamReader}
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     */
    public static ProcessorSupplier supplier(String host, int port) {
        return new Supplier(host, port);
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    private static void assertCountIsOne(int count) {
        if (count != 1) {
            throw new IllegalArgumentException("count != 1");
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String host;
        private final int port;

        private transient SocketTextStreamReader reader;

        Supplier(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            assertCountIsOne(count);
            this.reader = new SocketTextStreamReader(host, port);
            return singletonList(reader);
        }

        @Override
        public void complete(Throwable error) {
            uncheckRun(reader::close);
        }
    }

}
