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

import com.hazelcast.config.Config;
import com.hazelcast.core.Function;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicLongTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void get() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicLong an = hazelcastInstance.getAtomicLong("get");
        assertEquals(0, an.get());
    }

        @Test
    @ClientCompatibleTest
    public void testSimpleAtomicLong() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicLong an = hazelcastInstance.getAtomicLong("testAtomicLong");
        assertEquals(0, an.get());
        assertEquals(-1, an.decrementAndGet());
        assertEquals(0, an.incrementAndGet());
        assertEquals(1, an.incrementAndGet());
        assertEquals(2, an.incrementAndGet());
        assertEquals(1, an.decrementAndGet());
        assertEquals(1, an.getAndSet(23));
        assertEquals(28, an.addAndGet(5));
        assertEquals(28, an.get());
        assertEquals(28, an.getAndAdd(-3));
        assertEquals(24, an.decrementAndGet());
        Assert.assertFalse(an.compareAndSet(23, 50));
        assertTrue(an.compareAndSet(24, 50));
        assertTrue(an.compareAndSet(50, 0));
    }

    @Test
    @ClientCompatibleTest
    public void testMultipleThreadAtomicLong() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance[] instances = factory.newInstances(new Config());
        final HazelcastInstance instance = instances[0];
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
        try {
            countDownLatch.await(50, TimeUnit.SECONDS);
            Assert.assertEquals(0, atomicLong.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongFailure() {
        int k = 4;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(k + 1);
        Config config = new Config();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        String name = "testAtomicLongFailure";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        for (int i = 0; i < k; i++) {
            HazelcastInstance newInstance = nodeFactory.newHazelcastInstance(config);
            IAtomicLong newAtomicLong = newInstance.getAtomicLong(name);
            Assert.assertEquals((long) 100 + i, newAtomicLong.get());
            newAtomicLong.incrementAndGet();
            instance.getLifecycleService().shutdown();
            instance = newInstance;
        }
    }

    @Test
    @ClientCompatibleTest
    public void testAtomicLongSpawnNodeInParallel() throws InterruptedException {
        int total = 6;
        int parallel = 2;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(total + 1);
        final Config config = new Config();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        final String name = "testAtomicLongSpawnNodeInParallel";
        IAtomicLong atomicLong = instance.getAtomicLong(name);
        atomicLong.set(100);
        final ExecutorService ex = Executors.newFixedThreadPool(parallel);
        try {
            for (int i = 0; i < total / parallel; i++) {
                final HazelcastInstance[] instances = new HazelcastInstance[parallel];
                final CountDownLatch countDownLatch = new CountDownLatch(parallel);
                for (int j = 0; j < parallel; j++) {
                    final int id = j;
                    ex.execute(new Runnable() {
                        public void run() {
                            instances[id] = nodeFactory.newHazelcastInstance(config);
                            instances[id].getAtomicLong(name).incrementAndGet();
                            countDownLatch.countDown();
                        }
                    });
                }
                assertTrue(countDownLatch.await(1, TimeUnit.MINUTES));

                IAtomicLong newAtomicLong = instance.getAtomicLong(name);
                Assert.assertEquals((long) 100 + (i + 1) * parallel, newAtomicLong.get());
                instance.getLifecycleService().shutdown();
                instance = instances[0];
            }
        } finally {
            ex.shutdownNow();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    @ClientCompatibleTest
    public void apply_whenCalledWithNullFunction() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicLong ref = hazelcastInstance.getAtomicLong("apply_whenCalledWithNullFunction");

        ref.apply(null);
    }

    @Test
    @ClientCompatibleTest
    public void apply() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IAtomicLong ref = hazelcastInstance.getAtomicLong("apply");

        assertEquals(new Long(1), ref.apply(new AddOneFunction()));
        assertEquals(0, ref.get());
    }

    @Test
    @ClientCompatibleTest
    public void apply_whenException() {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("alter_whenCalledWithNullFunction");

       ref.alter(null);
   }

   @Test
   @ClientCompatibleTest
   public void alter_whenException() {
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("alter");

       ref.set(10);
       ref.alter(new AddOneFunction());
       assertEquals(11, ref.get());

   }

   @Test(expected = IllegalArgumentException.class)
   @ClientCompatibleTest
   public void alterAndGet_whenCalledWithNullFunction() {
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("alterAndGet_whenCalledWithNullFunction");

       ref.alterAndGet(null);
   }

   @Test
   @ClientCompatibleTest
   public void alterAndGet_whenException() {
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("alterAndGet");

       ref.set(10);
       assertEquals(11, ref.alterAndGet(new AddOneFunction()));
       assertEquals(11, ref.get());
  }

   @Test(expected = IllegalArgumentException.class)
   @ClientCompatibleTest
   public void getAndAlter_whenCalledWithNullFunction() {
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("getAndAlter_whenCalledWithNullFunction");

       ref.getAndAlter(null);
   }

   @Test
   @ClientCompatibleTest
   public void getAndAlter_whenException() {
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
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
       HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
       IAtomicLong ref = hazelcastInstance.getAtomicLong("getAndAlter");

       ref.set(10);
       assertEquals(10, ref.getAndAlter(new AddOneFunction()));
       assertEquals(11, ref.get());
   }

    private static class AddOneFunction implements Function<Long, Long> {
        @Override
        public Long apply(Long input) {
            return input+1;
        }
    }


    private static class FailingFunction implements Function<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new WoohaaException();
        }
    }

    private static class WoohaaException extends RuntimeException {

    }
}
