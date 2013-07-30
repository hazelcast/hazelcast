package com.hazelcast.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mdogan 7/30/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ConnectionTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
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
        Config c = new Config();
        final int port = 5701;
        c.getNetworkConfig().setPort(port).setPortAutoIncrement(false);

        if (withSocketInterceptor) {
            c.getNetworkConfig().setSocketInterceptorConfig(new SocketInterceptorConfig().setEnabled(true)
                    .setImplementation(new MemberSocketInterceptor() {
                        public void init(SocketInterceptorConfig socketInterceptorConfig) {
                        }

                        public void onAccept(Socket acceptedSocket) throws IOException {
                        }

                        public void onConnect(Socket connectedSocket) throws IOException {
                        }
                    }));
        }

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        final ExecutorService ex = Executors.newCachedThreadPool();
        final int count = Math.min(Runtime.getRuntime().availableProcessors() * 25, 200);
        final CountDownLatch latch = new CountDownLatch(count);
        final CountDownLatch ll = new CountDownLatch(1);
        final AtomicInteger cc = new AtomicInteger();

        new Thread() {
            public void run() {
                try {
                    ll.await();
                    hz.getLifecycleService().terminate();
                } catch (InterruptedException ignored) {
                }
            }
        }.start();

        final Collection<Socket> sockets = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
        for (int i = 0; i < count; i++) {
            ex.execute(new Runnable() {
                public void run() {
                    try {
                        if (cc.incrementAndGet() == count / 25) {
                            ll.countDown();
                        }
                        Socket socket = new Socket();
                        sockets.add(socket);
                        socket.connect(new InetSocketAddress(port));
                        socket.getOutputStream().write(Protocols.CLUSTER.getBytes());
                        socket.getInputStream().read();
                    } catch (IOException ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        } finally {
            for (Socket socket : sockets) {
                try {
                    socket.close();
                } catch (Exception e) {
                }
            }
            ex.shutdownNow();
        }
    }
}
