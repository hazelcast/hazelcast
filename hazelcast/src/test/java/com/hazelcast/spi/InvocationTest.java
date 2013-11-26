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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 9/16/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationTest extends HazelcastTestSupport {

    @Test
    public void testInterruptionDuringBlockingOp1() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final IQueue<Object> q = hz.getQueue("queue");

        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(interruptedFlag, latch) {
            protected void doOp() {
                try {
                    assertNotNull(q.poll(1, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        q.offer("new item!");

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertTrue(interruptedFlag.get());
    }

    @Test
    public void testInterruptionDuringBlockingOp2() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final ILock lock = hz.getLock("lock");
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());

        final AtomicBoolean interruptedFlag = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(interruptedFlag, latch) {
            protected void doOp() {
                try {
                    assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
                    assertTrue(lock.isLockedByCurrentThread());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        lock.unlock();

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertTrue(interruptedFlag.get());
    }

    private abstract class OpThread extends Thread {
        final AtomicBoolean interruptedFlag;
        final CountDownLatch latch;

        protected OpThread(AtomicBoolean interruptedFlag, CountDownLatch latch) {
            this.interruptedFlag = interruptedFlag;
            this.latch = latch;
        }

        public void run() {
            doOp();
            interruptedFlag.set(isInterrupted());
            try {
                sleep(1000 * 60);
            } catch (InterruptedException e) {
                latch.countDown();
            }
        }

        protected abstract void doOp();
    }
}
