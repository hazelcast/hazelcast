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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicLongTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void testGet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(0,atomicLong.get());
    }

    @Test
    @ClientCompatibleTest
    public void testDecrementAndGet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(-1,atomicLong.decrementAndGet());
        assertEquals(-2,atomicLong.decrementAndGet());
    }

    @Test
    @ClientCompatibleTest
    public void testIncrementAndGet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(1,atomicLong.incrementAndGet());
    }

    @Test
    @ClientCompatibleTest
    public void testGetAndSet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(0,atomicLong.getAndSet(271));
        assertEquals(271,atomicLong.get());
    }

    @Test
    @ClientCompatibleTest
    public void testAddAndGet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(271,atomicLong.addAndGet(271));
    }

    @Test
    @ClientCompatibleTest
    public void testGetAndAdd(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertEquals(0,atomicLong.getAndAdd(271));
        assertEquals(271,atomicLong.get());
    }

    @Test
    @ClientCompatibleTest
    public void testCompareAndSet(){
        HazelcastInstance hzInstance = createHazelcastInstance();
        IAtomicLong atomicLong = hzInstance.getAtomicLong("testAtomicLong");
        assertTrue(atomicLong.compareAndSet(0, 271));
        assertEquals(271,atomicLong.get());
        assertFalse(atomicLong.compareAndSet(172, 0));
    }

    @Test
    @ClientCompatibleTest
    public void testMultipleThreadAtomicLong() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final int k = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(k);
        final IAtomicLong atomicLong = instance.getAtomicLong("testMultipleThreadAtomicLong");
        for (int i = 0; i < k; i++) {
            new Thread() {
                public void run() {
                    long delta = (long) (Math.random() * 1000);
                    for (int j = 0; j < 10000; j++) {
                        atomicLong.addAndGet(delta);
                    }
                    for (int j = 0; j < 10000; j++) {
                        atomicLong.addAndGet(-1 * delta);
                    }
                    countDownLatch.countDown();
                }
            }.start();
        }
        assertOpenEventually(countDownLatch, 50);
        assertEquals(0, atomicLong.get());
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        String name = "testAtomicLongFailure";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance();
            IAtomicLong newAtomicLong = newInstance.getAtomicLong(name);
            assertEquals((long) 100 + i, newAtomicLong.get());
            newAtomicLong.incrementAndGet();
            instance.shutdown();
            instance = newInstance;
        }
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongSpawnNodeInParallel() throws InterruptedException {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        final String name = "testAtomicLongSpawnNodeInParallel";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        final ExecutorService ex = Executors.newFixedThreadPool(parallel);
        try {
            for (int i = 0; i < total / parallel; i++) {
                final HazelcastInstance[] instances = new HazelcastInstance[parallel];
                final CountDownLatch countDownLatch = new CountDownLatch(parallel);
                final AtomicInteger exceptionCount = new AtomicInteger(0);
                for (int j = 0; j < parallel; j++) {
                    final int id = j;
                    ex.execute(new Runnable() {
                        public void run() {
                            try {
                                instances[id] = nodeFactory.newHazelcastInstance();
                                instances[id].getAtomicLong(name).incrementAndGet();
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                                e.printStackTrace();
                            } finally {
                                countDownLatch.countDown();
                            }
                        }
                    });
                }
                assertOpenEventually(countDownLatch);

                // if there is an exception while incrementing in parallel threads, find number of exceptions
                // and subtract the number from expectedValue.
                final int thrownExceptionCount = exceptionCount.get();
                final long expectedValue = (long) 100 + (i + 1) * parallel - thrownExceptionCount;
                IAtomicLong newAtomicLong = instance.getAtomicLong(name);
                assertEquals(expectedValue, newAtomicLong.get());
                instance.shutdown();
                instance = instances[0];
            }
        } finally {
            ex.shutdownNow();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void apply_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("apply_whenCalledWithNullFunction");

        ref.apply(null);
    }

    @Test
    @ClientCompatibleTest
    public void apply() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("apply");

        assertEquals(new Long(1), ref.apply(new AddOneFunction()));
        assertEquals(0, ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void apply_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("apply");
        ref.set(1);
        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(1, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void alter_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alter_whenCalledWithNullFunction");

        ref.alter(null);
    }

    @Test
    @ClientCompatibleTest
    public void alter_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alter_whenException");
        ref.set(10);

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void alter() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alter");

        ref.set(10);
        ref.alter(new AddOneFunction());
        assertEquals(11, ref.get());

    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void alterAndGet_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alterAndGet_whenCalledWithNullFunction");

        ref.alterAndGet(null);
    }

    @Test
    @ClientCompatibleTest
    public void alterAndGet_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alterAndGet_whenException");
        ref.set(10);

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void alterAndGet() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("alterAndGet");

        ref.set(10);
        assertEquals(11, ref.alterAndGet(new AddOneFunction()));
        assertEquals(11, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void getAndAlter_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("getAndAlter_whenCalledWithNullFunction");

        ref.getAndAlter(null);
    }

    @Test
    @ClientCompatibleTest
    public void getAndAlter_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("getAndAlter_whenException");
        ref.set(10);

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (WoohaaException expected) {
        }

        assertEquals(10, ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void getAndAlter() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IAtomicLong ref = hazelcastInstance.getAtomicLong("getAndAlter");

        ref.set(10);
        assertEquals(10, ref.getAndAlter(new AddOneFunction()));
        assertEquals(11, ref.get());
    }

    private static class AddOneFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            return input + 1;
        }
    }


    private static class FailingFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new WoohaaException();
        }
    }

    private static class WoohaaException extends RuntimeException {

    }
}
