/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client.longrunning;

import com.hazelcast.client.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastClientPerformanceTest extends HazelcastClientTestBase {

    @Test
    public void putAndget100000RecordsWith1ClusterMember() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> map = hClient.getMap("putAndget100000RecordsWith1ClusterMember");
        putAndGet(map, 100000);
    }

    private void putAndGet(Map<String, String> map, int counter) {
        long beginTime = Clock.currentTimeMillis();
        for (int i = 1; i <= counter; i++) {
            if (i % (counter / 10) == 0) {
                System.out.println(i + ": " + (Clock.currentTimeMillis() - beginTime) + " ms");
            }
            map.put("key_" + i, String.valueOf(i));
        }
        beginTime = Clock.currentTimeMillis();
        for (int i = 1; i <= counter; i++) {
            if (i % (counter / 10) == 0) {
                System.out.println(i + ": " + (Clock.currentTimeMillis() - beginTime) + " ms");
            }
            assertEquals(String.valueOf(i), map.get("key_" + i));
        }
    }

    @Test
    public void putAndget100000RecordsWith1ClusterMemberFrom10Threads() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final Map<String, String> map = hClient.getMap("putAndget100000RecordsWith1ClusterMemberFrom10Threads");
        int count = 100000;
        int threads = 16;
        final AtomicInteger getCounter = new AtomicInteger(count);
        final AtomicInteger putCounter = new AtomicInteger(count);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        final long beginTime = Clock.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    int i;
                    while ((i = putCounter.getAndDecrement()) > 0) {
                        map.put("key_" + i, String.valueOf(i));
                    }
                    while ((i = getCounter.getAndDecrement()) > 0) {
                        map.get("key_" + i);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println(threads + " Threads made in total " + count +
                " puts and gets in " + (Clock.currentTimeMillis() - beginTime) + " ms");
    }

    @Test
    public void putFromMultipleThreads() throws InterruptedException {
        final HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        final AtomicInteger counter = new AtomicInteger(0);
        class Putter implements Runnable {
            volatile Boolean run = true;

            public void run() {
                HazelcastClient hClient = TestUtility.newHazelcastClient(h);
                while (run) {
                    Map<String, String> clientMap = hClient.getMap("putFromMultipleThreads");
                    clientMap.put(String.valueOf(counter.incrementAndGet()), String.valueOf(counter.get()));
                }
            }
        }
        ;
        List<Putter> list = new ArrayList<Putter>();
        for (int i = 0; i < 10; i++) {
            Putter p = new Putter();
            list.add(p);
            new Thread(p).start();
        }
        Thread.sleep(5000);
        for (Iterator<Putter> it = list.iterator(); it.hasNext(); ) {
            Putter p = it.next();
            p.run = false;
        }
        Thread.sleep(100);
        assertEquals(counter.get(), h.getMap("putFromMultipleThreads").size());
    }

    @Test
    public void putBigObject() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, Object> clientMap = hClient.getMap("putABigObject");
        List list = new ArrayList();
        int size = 10000000;
        byte[] b = new byte[size];
        b[size - 1] = (byte) 144;
        list.add(b);
        clientMap.put("obj", b);
        byte[] bigB = (byte[]) clientMap.get("obj");
        assertTrue(Arrays.equals(b, bigB));
        assertEquals(size, bigB.length);
    }

    @Test
    @Ignore
    public void testOutThreadPerformance() throws IOException, InterruptedException {
        new Thread(new Runnable() {
            public void run() {
                ServerSocket serverSocket = null;
                try {
                    serverSocket = new ServerSocket(5799);
                } catch (IOException e) {
                    System.out.println("Could not listen on port: 4444");
                    System.exit(-1);
                }
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    byte[] bytes = new byte[1000000];
                    while (true) {
                        clientSocket.getInputStream().read(bytes);
                    }
                } catch (IOException e) {
                    System.out.println("Accept failed: 4444");
                    System.exit(-1);
                }
            }
        }).start();
        HazelcastClient client = mock(HazelcastClient.class);
        ConnectionManager connectionManager = mock(ConnectionManager.class);
        when(client.getConnectionManager()).thenReturn(connectionManager);
        Connection connection = new Connection("localhost", 5799, 1);
        when(connectionManager.getConnection()).thenReturn(connection);
        ProtocolWriter packetWriter = new ProtocolWriter();
        packetWriter.setConnection(connection);
        final OutRunnable outRunnable = new OutRunnable(client, new HashMap<Long, Call>(), packetWriter);
        new Thread(outRunnable).start();
        final AtomicLong callCounter = new AtomicLong();
        final long start = Clock.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        final BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
        final Object object = new Object();
        for (int i = 0; i < 16; i++) {
            executorService.execute(new Runnable() {

                public void run() {
                    for (; ; ) {
                        try {
                            queue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
//                        Packet packet = new Packet();
//                        packet.set("c:default", ClusterOperation.CONCURRENT_MAP_GET, new byte[30], null);
//                        packet.setCallId(callCounter.incrementAndGet());
//                        Call call = new Call(packet.getCallId(), packet);
//                        outRunnable.enQueue(call);
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                int numberOfTasks = 10000;
//                int numberOfTasks = 11000;
                while (true) {
                    try {
                        for (int i = 0; i < numberOfTasks; i++) {
                            queue.offer(object);
                        }
//                        numberOfTasks = numberOfTasks + numberOfTasks/10;
                        Thread.sleep(1 * 1000);
                        System.out.println("Operations per millisecond : " + callCounter.get() / (Clock.currentTimeMillis() - start));
                        System.out.println("out runnable Queue size: " + outRunnable.getQueueSize());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        Thread.sleep(1000000);
    }

    @AfterClass
    @BeforeClass
    public static void shutdown() {
//        getHazelcastClient().shutdown();
//        Hazelcast.shutdownAll();
//        TestUtility.client = null;
    }
}
