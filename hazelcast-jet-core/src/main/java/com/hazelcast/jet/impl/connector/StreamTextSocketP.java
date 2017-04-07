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
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @see com.hazelcast.jet.Processors#streamTextSocket(String, int, Charset)
 */
public class StreamTextSocketP extends AbstractProcessor {

    private final String host;
    private final int port;
    private final Charset charset;

    StreamTextSocketP(String host, int port, Charset charset) {
        this.host = host;
        this.port = port;
        this.charset = charset;
    }

    @Override
    public boolean complete() {
        try {
            getLogger().info("Connecting to socket " + hostAndPort());
            try (
                    Socket socket = new Socket(host, port);
                    BufferedReader bufferedReader =
                            new BufferedReader(new InputStreamReader(socket.getInputStream(), charset))
            ) {
                getLogger().info("Connected to socket " + hostAndPort());

                for (String line; (line = bufferedReader.readLine()) != null; ) {
                    emit(line);
                }

                getLogger().info("Closing socket " + hostAndPort());
            }

            return true;
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    public static Supplier<Processor> supplier(String host, int port, String charset) {
        Charset charsetObj = charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset);
        return () -> new StreamTextSocketP(host, port, charsetObj);
    }
}
