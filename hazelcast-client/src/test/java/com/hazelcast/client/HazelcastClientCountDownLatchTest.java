/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.*;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.Address;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClientCountDownLatchTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientCountDownLatchSimple() throws InterruptedException, IOException {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        final ICountDownLatch cdl1 = client1.getCountDownLatch("test");
        final ICountDownLatch cdl2 = client2.getCountDownLatch("test");
        final Member c1Member = clientToMember(client1);
        final AtomicInteger result = new AtomicInteger();
        int count = 5;
        cdl1.setCount(count);
        assertEquals(c1Member, ((CountDownLatchClientProxy) cdl2).getOwner());
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    // should succeed
                    if (cdl2.await(1000, TimeUnit.MILLISECONDS))
                        result.incrementAndGet();
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail();
                }
            }
        };
        thread.start();
        for (int i = count; i > 0; i--) {
            assertEquals(i, ((CountDownLatchClientProxy) cdl2).getCount());
            cdl1.countDown();
            Thread.sleep(100);
        }
        assertEquals(1, result.get());
    }

    @Test
    public void testClientCountDownLatchOwnerLeft() throws InterruptedException, IOException {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        final ICountDownLatch cdl1 = client1.getCountDownLatch("test");
        final ICountDownLatch cdl2 = client2.getCountDownLatch("test");
        final Member c1Member = clientToMember(client1);
        final AtomicInteger result = new AtomicInteger();
        cdl1.setCount(1);
        assertEquals(1, ((CountDownLatchClientProxy) cdl2).getCount());
        assertEquals(c1Member, ((CountDownLatchClientProxy) cdl2).getOwner());
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    // should throw MemberLeftException
                    cdl2.await(1000, TimeUnit.MILLISECONDS);
                    fail();
                } catch (MemberLeftException e) {
                    result.incrementAndGet();
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail();
                }
            }
        };
        thread.start();
        Thread.sleep(200);
        client1.shutdown();
        thread.join();
        assertEquals(1, result.get());
    }

    @Test
    public void testClientCountDownLatchInstanceDestroyed() throws InterruptedException, IOException {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        final ICountDownLatch cdl1 = client1.getCountDownLatch("test");
        final ICountDownLatch cdl2 = client2.getCountDownLatch("test");
        final Member c1Member = clientToMember(client1);
        final AtomicInteger result = new AtomicInteger();
        cdl1.setCount(1);
        assertEquals(1, ((CountDownLatchClientProxy) cdl2).getCount());
        assertEquals(c1Member, ((CountDownLatchClientProxy) cdl2).getOwner());
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    // should throw InstanceDestroyedException
                    cdl2.await(1000, TimeUnit.MILLISECONDS);
                    fail();
                } catch (InstanceDestroyedException e) {
                    result.incrementAndGet();
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail();
                }
            }
        };
        thread.start();
        Thread.sleep(200);
        cdl1.destroy();
        thread.join();
        assertEquals(1, result.get());
    }

    @Test
    public void testClientCountDownLatchClientShutdown() throws InterruptedException, IOException {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        HazelcastClient client1 = newHazelcastClient(instance);
        HazelcastClient client2 = newHazelcastClient(instance);
        final ICountDownLatch cdl1 = client1.getCountDownLatch("test");
        final ICountDownLatch cdl2 = client2.getCountDownLatch("test");
        final Member c1Member = clientToMember(client1);
        final AtomicInteger result = new AtomicInteger();
        cdl1.setCount(1);
        assertEquals(1, ((CountDownLatchClientProxy) cdl2).getCount());
        assertEquals(c1Member, ((CountDownLatchClientProxy) cdl2).getOwner());
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    // should throw IllegalStateException
                    cdl1.await(1000, TimeUnit.MILLISECONDS);
                } catch (IllegalStateException e) {
                    result.incrementAndGet();
                    return;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                fail();
            }
        };
        thread.start();
        Thread.sleep(20);
        client1.shutdown();
        thread.join();
        assertEquals(1, result.get());
    }

    // remove this when Clients become members
    private Member clientToMember(HazelcastClient client) throws IOException {
        final Socket socket1 = client.connectionManager.getConnection().getSocket();
        final Address client1Address = new Address(socket1.getLocalAddress(), socket1.getLocalPort());
        return new MemberImpl(client1Address, false);
    }
}
