/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.DefaultSSLEngineFactory;
import com.hazelcast.internal.tpcengine.net.DevNullAsyncSocketReader;

import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TLSTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public ReactorBuilder newReactorBuilder() {
        return new NioReactorBuilder();
    }

    public Reactor newReactor() {
        ReactorBuilder builder = newReactorBuilder();
        Reactor reactor = builder.build();
        reactors.add(reactor);
        return reactor.start();
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test() throws InterruptedException {
        DefaultSSLEngineFactory sslEngineFactory = new DefaultSSLEngineFactory();
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                            .setReader(new DevNullAsyncSocketReader())
                            .setSSLEngineFactory(sslEngineFactory)
                            .build();
                    socket.start();
                })
                .build();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.start();

        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .setSSLEngineFactory(sslEngineFactory)
                .build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverAddress, clientSocket.getRemoteAddress());

        Thread.sleep(2000);

        for (int k = 0; k < 100; k++) {
            IOBuffer ioBuffer = new IOBuffer(8);
            ioBuffer.writeLong(1);
            ioBuffer.flip();
            clientSocket.writeAndFlush(ioBuffer);
        }
        Thread.sleep(15000);
    }
}
