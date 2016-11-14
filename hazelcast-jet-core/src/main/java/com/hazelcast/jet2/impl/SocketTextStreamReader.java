/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import javax.annotation.Nonnull;

import static java.util.Collections.singletonList;

/**
 * A producer reads lines from socket and emits them as string to the next processor
 */
public class SocketTextStreamReader extends AbstractProducer {

    private static final ILogger LOGGER = com.hazelcast.logging.Logger.getLogger(SocketTextStreamReader.class);
    private final String host;
    private final int port;
    private Socket socket;
    private BufferedReader bufferedReader;

    protected SocketTextStreamReader(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        try {
            LOGGER.info("Connecting to the socket -> " + host + ":" + port);
            socket = new Socket(host, port);
            LOGGER.info("Connected to the socket -> " + host + ":" + port);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        } catch (IOException e) {
            ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean complete() {
        try {
            String inputLine;
            while ((inputLine = bufferedReader.readLine()) != null) {
                emit(inputLine);
                if (getOutbox().isHighWater()) {
                    return false;
                }
            }
        } catch (IOException e) {
            LOGGER.severe("IO error occurred, closing the reader, error : " + e.getMessage());
            return true;
        }
        LOGGER.info("Closed the socket -> " + host + ":" + port);
        return true;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    /**
     * Creates a supplier for {@link SocketTextStreamReader}
     *
     * @param host The host name to connect
     * @param port The port number to connect
     */
    public static ProcessorSupplier supplier(String host, int port) {
        return new Supplier(host, port);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String host;
        private final int port;

        Supplier(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public List<Processor> get(int count) {
            assertCountIsOne(count);
            return singletonList(new SocketTextStreamReader(host, port));
        }
    }

    private static void assertCountIsOne(int count) {
        if (count != 1) {
            throw new IllegalArgumentException("count != 1");
        }
    }


}
