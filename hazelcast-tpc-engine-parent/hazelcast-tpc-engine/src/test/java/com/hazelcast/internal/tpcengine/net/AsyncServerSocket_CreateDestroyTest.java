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
import com.hazelcast.internal.tpcengine.util.ExceptionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies that an AsyncServerSocket can be created and destroyed many
 * times after each other. The NioReactor should be fine since the resources
 * will be gc's by the JVM automatically, but it will help to flush out problems
 * in the UringReactor because of unsafe memory allocation, OS resources that need
 * to be released like the file descriptors etc.
 * <p>
 * There are short running tests for the regular tests, and there are also nightly
 * version that run much longer.
 */
public abstract class AsyncServerSocket_CreateDestroyTest {

    protected long iterations = 100;
    private Reactor reactor;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        Reactor.Builder builder = newReactorBuilder();
        // Make sure that we create a uring instance that doesn't
        // have a lot of capacity. This will flush out problems faster.
        // And this also creates just a little bit of space for listeners
        // in the completion queue. Exposing problems faster.
        builder.socketsLimit = 1;
        builder.fileLimit = 1;
        builder.storagePendingLimit = 1;
        builder.storageSubmitLimit = 1;
        builder.serverSocketsLimit = 1;
        reactor = builder.build();
        reactor.start();
    }

    @After
    public void after() throws InterruptedException {
        terminate(reactor);
    }

    @Test
    public void test() {
        for (long iteration = 0; iteration < iterations; iteration++) {
            if (iteration % 1000 == 0) {
                System.out.println("at iteration:" + iteration);
            }

            try {
                AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
                serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
                serverSocketBuilder.acceptFn = acceptRequest -> {
                    AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
                    socketBuilder.reader = new DevNullAsyncSocketReader();
                    AsyncSocket socket = socketBuilder.build();
                    socket.start();
                };

                AsyncServerSocket serverSocket = serverSocketBuilder.build();
                serverSocket.start();
                serverSocket.close();

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(0, reactor.serverSockets().size());
                    }
                });
            } catch (Throwable t) {
                System.out.println("Problem detected at iteration " + iteration + ".");
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }
}
