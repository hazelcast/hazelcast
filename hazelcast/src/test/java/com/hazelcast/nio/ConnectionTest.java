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

package com.hazelcast.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.management.ThreadDumpGenerator.dumpAllThreads;
import static org.junit.Assert.assertTrue;

/**
 * IGNORED THIS TEST COMPLETELY. KEEPING FOR FUTURE REFERENCE.
 * PRONE TO FAIL BECAUSE OF BLOCKING TCP CONNECT-ACCEPT-CLOSE CYCLE.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore("See testBlockedClientSockets and testBlockedClientSockets2 tests. Currently we couldn't find a way to make them pass...")
public class ConnectionTest extends HazelcastTestSupport {

    @BeforeClass
    public static void init() {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testBlockedClientSockets() throws Exception {
        final ServerSocket serverSocket = new ServerSocket(13131, 1);
        final int count = 100;
        final CountDownLatch latch = new CountDownLatch(count);
        final AtomicInteger connected = new AtomicInteger();
        final AtomicInteger cc = new AtomicInteger();

        final Set<Socket> sockets = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
        final Thread st = new Thread("server-socket") {
            public void run() {
                while (!isInterrupted()) {
                    try {
                        Socket socket = serverSocket.accept();
                        sockets.add(socket);
                    } catch (IOException ignored) {
                    }
                }
            }
        };
        st.start();

        final AtomicBoolean flag = new AtomicBoolean(false);
        for (int i = 0; i < count; i++) {
            final Socket clientSocket = new Socket();
            Thread t = new Thread("client-socket-" + i) {
                public void run() {
                    try {
                        if (cc.incrementAndGet() > count / 5 && Math.random() > .87f && flag.compareAndSet(false, true)) {
                            st.interrupt();
                            serverSocket.close();
                            try {
                                st.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            Iterator<Socket> iter = sockets.iterator();
                            while (iter.hasNext()) {
                                Socket socket = iter.next();
                                socket.shutdownOutput();
                                socket.close();
                                iter.remove();
                            }
                        } else {
                            clientSocket.connect(new InetSocketAddress(13131));
                            connected.incrementAndGet();
                            clientSocket.getInputStream().read();
                        }
                    } catch (IOException ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            };
            t.setDaemon(true);
            t.start();
        }

        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testBlockedClientSockets2() throws Exception {
        final ServerSocket serverSocket = new ServerSocket(13131);
        final int count = 100;
        final CountDownLatch latch = new CountDownLatch(count);
        final AtomicInteger connected = new AtomicInteger();
        final AtomicInteger cc = new AtomicInteger();

        final AtomicBoolean flag = new AtomicBoolean(false);

        for (int i = 0; i < count; i++) {
            final Socket clientSocket = new Socket();
            Thread t = new Thread("client-socket-" + i) {
                public void run() {
                    try {
                        if (cc.incrementAndGet() > count / 5 && Math.random() > .87f && flag.compareAndSet(false, true)) {
                            serverSocket.close();
                        } else {
                            clientSocket.setSoTimeout(1000 * 5);
                            clientSocket.connect(new InetSocketAddress(13131));
                            connected.incrementAndGet();
                            InputStream in = clientSocket.getInputStream();
                            in.read();
                        }
                    } catch (IOException ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            };
            t.setDaemon(true);
            t.start();
        }

        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testDanglingSocketsOnTerminate() throws Exception {
        testDanglingSocketsOnTerminate(false);
    }

    @Test
    public void testDanglingSocketsOnTerminate2() throws Exception {
        testDanglingSocketsOnTerminate(true);
    }

    private void testDanglingSocketsOnTerminate(boolean withSocketInterceptor) throws Exception {
        final int port = 5701;
        Config config = new Config();
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);

        if (withSocketInterceptor) {
            config.getNetworkConfig().setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(true)
                    .setImplementation(new MemberSocketInterceptor() {
                        public void init(Properties properties) {
                        }

                        public void onAccept(Socket acceptedSocket) throws IOException {
                        }

                        public void onConnect(Socket connectedSocket) throws IOException {
                        }
                    }));
        }

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final int count = 100;
        final CountDownLatch latch = new CountDownLatch(count);
        final CountDownLatch ll = new CountDownLatch(1);
        final AtomicInteger cc = new AtomicInteger();

        new Thread() {
            public void run() {
                try {
                    ll.await(1, TimeUnit.MINUTES);
                    hz.getLifecycleService().terminate();
                } catch (InterruptedException ignored) {
                }
            }
        }.start();

        final Collection<Socket> sockets = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
        final AtomicInteger k0 = new AtomicInteger();
        final AtomicInteger k1 = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            Runnable task = new Runnable() {
                public void run() {
                    try {
                        if (cc.incrementAndGet() == count / 10) {
                            ll.countDown();
                        }
                        Socket socket = new Socket();
                        sockets.add(socket);
                        try {
                            socket.connect(new InetSocketAddress(port));
                            k0.incrementAndGet();
                        } catch (IOException e) {
                            k1.incrementAndGet();
                        }

                        OutputStream out = socket.getOutputStream();
                        out.write(Protocols.CLUSTER.getBytes());
                        out.flush();

                        socket.getInputStream().read();
                    } catch (IOException ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            };
            Thread t = new Thread(task, "socket-thread-" + i);
            t.setDaemon(true);
            t.start();
        }

        try {
            assertTrue(latch.await(1, TimeUnit.MINUTES));
        } catch (AssertionError e) {
            System.err.println(dumpAllThreads());
            throw e;
        } finally {
            for (Socket socket : sockets) {
                try {
                    socket.close();
                } catch (Exception e) {
                    ignore(e);
                }
            }
        }
    }
}
