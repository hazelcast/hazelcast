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

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClientRunnableTest {
    @Test
    public void testRun() throws Exception {
        final CountDownLatch waitLatch = new CountDownLatch(1);
        final ClientRunnable clientRunnable = new ClientRunnable() {
            @Override
            protected void customRun() throws InterruptedException {
                waitLatch.countDown();
            }
        };
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    waitLatch.await(10, TimeUnit.SECONDS);
                    synchronized (clientRunnable.monitor) {
                        clientRunnable.running = false;
                        clientRunnable.monitor.wait();
                    }
                    latch.countDown();
                } catch (InterruptedException e) {
                }
            }
        }).start();
        clientRunnable.run();
        assertTrue("Not notified", latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testShutdown() throws Exception {
        final ClientRunnable clientRunnable = new ClientRunnable() {
            @Override
            protected void customRun() throws InterruptedException {
            }
        };
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latchShutDown = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    latch1.await();
                    latch2.countDown();
                    clientRunnable.shutdown();
                    latchShutDown.countDown();
                } catch (InterruptedException e) {
                }
            }
        }).start();
        latch1.countDown();
        latch2.await();
        Thread.sleep(10);
        clientRunnable.terminate();
        assertTrue(latchShutDown.await(5, TimeUnit.SECONDS));
        assertFalse(clientRunnable.running);
    }
}
