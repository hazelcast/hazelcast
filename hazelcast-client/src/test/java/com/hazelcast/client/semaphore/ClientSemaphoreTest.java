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

package com.hazelcast.client.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.*;

/**
 * @author ali 5/24/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSemaphoreTest {

    static final String name = "test1";
    static HazelcastInstance client;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static ISemaphore s;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        s = client.getSemaphore(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        s.reducePermits(100);
        s.release(10);
    }

    @Test
    public void testAcquire() throws Exception {

        assertEquals(10, s.drainPermits());

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    s.acquire();
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);

        s.release(2);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(1, s.availablePermits());

    }

    @Test
    public void tryAcquire() throws Exception {
        assertTrue(s.tryAcquire());
        assertTrue(s.tryAcquire(9));
        assertEquals(0, s.availablePermits());
        assertFalse(s.tryAcquire(1, TimeUnit.SECONDS));
        assertFalse(s.tryAcquire(2, 1, TimeUnit.SECONDS));


        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(s.tryAcquire(2, 5, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        s.release(2);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, s.availablePermits());

    }


    @Test
    public void concurrent_trySemaphoreTest() {
        concurrent_trySemaphoreTest(false);
    }

    @Test
    public void concurrent_trySemaphoreWithTimeOutTest() {
        concurrent_trySemaphoreTest(true);
    }

    public void concurrent_trySemaphoreTest(boolean tryWithTimeOut) {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(1);

        final AtomicInteger upTotal = new AtomicInteger(0);
        final AtomicInteger downTotal = new AtomicInteger(0);

        SemaphoreTestThread threads[] = new SemaphoreTestThread[8];
        for(int i=0; i<threads.length; i++){

            SemaphoreTestThread t;

            if(tryWithTimeOut){
                t = new TrySemaphoreTimeOutThread(semaphore, upTotal, downTotal);
            }
            else{
                t = new TrySemaphoreThread(semaphore, upTotal, downTotal);
            }

            t.start();
            threads[i] = t;
        }
        HazelcastTestSupport.assertJoinable(threads);

        for(SemaphoreTestThread t : threads){
            assertNull("thread "+ t +" has error "+t.error, t.error);
        }

        assertTrue("concurrent access to locked code caused wrong total", upTotal.get() + downTotal.get() == 0);
    }

    static abstract class SemaphoreTestThread extends Thread{
        static private final Random random = new Random();
        protected ISemaphore semaphore ;
        protected AtomicInteger upTotal;
        protected AtomicInteger downTotal;
        public Throwable error =null;

        public SemaphoreTestThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            this.semaphore = semaphore;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        public void run(){
            try{
                doRun();
            }catch (Throwable e){
                error = e;
            }
        }

        abstract void doRun() throws Exception;

        protected void work(){
            int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }

    static class TrySemaphoreThread extends SemaphoreTestThread{
        public TrySemaphoreThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            super(semaphore, upTotal, downTotal);
        }

        public void doRun() throws Exception{
            for ( int i=0; i<1000*10; i++ ) {
                if(semaphore.tryAcquire()){
                    work();
                    semaphore.release();
                }
            }
        }
    }

    static class TrySemaphoreTimeOutThread extends SemaphoreTestThread{
        public TrySemaphoreTimeOutThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            super(semaphore, upTotal, downTotal);
        }

        public void doRun() throws Exception{
            for ( int i=0; i<1000*10; i++ ) {
                if(semaphore.tryAcquire(10, TimeUnit.MILLISECONDS )){
                    work();
                    semaphore.release();
                }
            }
        }
    }
}
