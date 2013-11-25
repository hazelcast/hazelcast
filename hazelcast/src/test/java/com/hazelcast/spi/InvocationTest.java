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
import static org.junit.Assert.fail;

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

        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(latch) {
            protected void doOp()throws InterruptedException {
                q.poll(1, TimeUnit.MINUTES);
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        q.offer("new item!");

        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testInterruptionDuringBlockingOp2() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(new Config());
        final ILock lock = hz.getLock("lock");
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new OpThread(latch) {
            protected void doOp() throws InterruptedException {
                assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
            }
        };
        thread.start();

        Thread.sleep(5000);
        thread.interrupt();
        lock.unlock();

        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    private abstract class OpThread extends Thread {
        final CountDownLatch latch;

        protected OpThread(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run() {
            try {
                doOp();
            } catch (InterruptedException e) {
                latch.countDown();
            }
        }

        protected abstract void doOp()throws InterruptedException;
    }
}
