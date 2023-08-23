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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.AssertTask;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.ExceptionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.io.IOUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies that an AsyncSocket can be created and destroyed many
 * times after each other. The NioReactor should be fine since the resources
 * will be gc's by the JVM automatically, but it will help to flush out problems
 * in the UringReactor because of unsafe memory allocation, OS resources that need
 * to be released like the file descriptors etc.
 * <p>
 * There are short running tests for the regular tests, and there are also nightly
 * version that run much longer.
 */
public abstract class AsyncSocket_CreateDestroyTest {

    protected long iterations = 10;
    private Reactor serverReactor;
    private Reactor clientReactor;
    private AsyncServerSocket serverSocket;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        Reactor.Builder serverReactorBuilder = newReactorBuilder();
        // Make sure that we create a uring instance that doesn't
        // have a lot of capacity. This will flush out problems faster.
        // And this also creates just a little bit of space for listeners
        // in the completion queue. Exposing problems faster.
        serverReactorBuilder.socketsLimit = 2;
        serverReactorBuilder.fileLimit = 1;
        serverReactorBuilder.storagePendingLimit = 1;
        serverReactorBuilder.storageSubmitLimit = 1;
        serverReactorBuilder.serverSocketsLimit = 1;
        serverReactor = serverReactorBuilder.build();
        serverReactor.start();

        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("192.168.1.134", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };

        serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        Reactor.Builder clientReactorBuilder = newReactorBuilder();
        // Make sure that we create a uring instance that doesn't
        // have a lot of capacity. This will flush out problems faster.
        // And this also creates just a little bit of space for listeners
        // in the completion queue. Exposing problems faster.
        clientReactorBuilder.socketsLimit = 2;
        clientReactorBuilder.fileLimit = 1;
        clientReactorBuilder.storagePendingLimit = 1;
        clientReactorBuilder.storageSubmitLimit = 1;
        clientReactorBuilder.serverSocketsLimit = 1;
        clientReactor = clientReactorBuilder.build();
        clientReactor.start();
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(asList(serverReactor, clientReactor));
    }


    @Test
    public void test_whenServerSideClose() {
        for (long iteration = 0; iteration < iterations; iteration++) {
            if (iteration % 1000 == 0) {
                System.out.println("at iteration:" + iteration);
            }

            try {
                AsyncSocket.Builder localSocketBuilder = clientReactor.newAsyncSocketBuilder();
                localSocketBuilder.reader = new DevNullAsyncSocketReader();
                AsyncSocket localSocket = localSocketBuilder.build();
                localSocket.start();

                SocketAddress localAddress = serverSocket.getLocalAddress();
                localSocket.connect(localAddress).join();

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(1, clientReactor.sockets().size());
                        assertEquals(1, serverReactor.sockets().size());
                    }
                });

                localSocket.close();

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(0, serverReactor.sockets().size());
                        assertEquals(0, clientReactor.sockets().size());
                    }
                });

                assertEquals(1, serverReactor.serverSockets().size());
            } catch (Throwable t) {
                System.out.println("Problem detected at iteration " + iteration + ".");
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }

    @Test
    public void test_whenClientSideClose() {
        for (long iteration = 0; iteration < iterations; iteration++) {
            if (iteration % 1000 == 0) {
                System.out.println("at iteration:" + iteration);
            }

            try {
                AsyncSocket.Builder localSocketBuilder = clientReactor.newAsyncSocketBuilder();
                localSocketBuilder.reader = new DevNullAsyncSocketReader();
                AsyncSocket localSocket = localSocketBuilder.build();
                localSocket.start();

                SocketAddress localAddress = serverSocket.getLocalAddress();
                localSocket.connect(localAddress).join();

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(1, clientReactor.sockets().size());
                        assertEquals(1, serverReactor.sockets().size());
                    }
                });

                localSocket.close();

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(0, serverReactor.sockets().size());
                        assertEquals(0, clientReactor.sockets().size());
                    }
                });

                assertEquals(1, serverReactor.serverSockets().size());
            } catch (Throwable t) {
                System.out.println("Problem detected at iteration " + iteration + ".");
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }
}
