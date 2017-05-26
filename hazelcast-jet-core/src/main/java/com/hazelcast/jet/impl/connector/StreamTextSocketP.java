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
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @see Sources#streamTextSocket(String, int, Charset)
 */
public final class StreamTextSocketP extends AbstractProcessor {

    private final String host;
    private final int port;
    private final Charset charset;
    private CompletableFuture<Void> jobFuture;

    private StreamTextSocketP(String host, int port, Charset charset) {
        this.host = host;
        this.port = port;
        this.charset = charset;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        jobFuture = context.jobFuture();
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

                for (String line; !jobFuture.isDone() && (line = bufferedReader.readLine()) != null; ) {
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

    public static DistributedSupplier<Processor> supplier(String host, int port, @Nonnull String charset) {
        return () -> new StreamTextSocketP(host, port, Charset.forName(charset));
    }
}
