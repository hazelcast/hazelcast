package com.hazelcast.client.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientSemaphoreThreadedTest {

    protected static HazelcastInstance client;
    protected static HazelcastInstance server;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void concurrent_trySemaphoreTest() {
        concurrent_trySemaphoreTest(false);
    }

    @Test
    public void concurrent_trySemaphoreWithTimeOutTest() {
        concurrent_trySemaphoreTest(true);
    }

    public void concurrent_trySemaphoreTest(final boolean tryWithTimeOut) {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(1);
        final AtomicInteger upTotal = new AtomicInteger(0);
        final AtomicInteger downTotal = new AtomicInteger(0);

        final SemaphoreTestThread threads[] = new SemaphoreTestThread[8];
        for(int i=0; i<threads.length; i++){
            SemaphoreTestThread t;
            if(tryWithTimeOut){
                t = new TrySemaphoreTimeOutThread(semaphore, upTotal, downTotal);
            }else{
                t = new TrySemaphoreThread(semaphore, upTotal, downTotal);
            }
            t.start();
            threads[i] = t;
        }
        HazelcastTestSupport.assertJoinable(threads);

        for(SemaphoreTestThread t : threads){
            assertNull("thread "+ t +" has error "+t.error, t.error);
        }

        assertEquals("concurrent access to locked code caused wrong total", 0, upTotal.get() + downTotal.get());
    }

    static class TrySemaphoreThread extends SemaphoreTestThread{
        public TrySemaphoreThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            super(semaphore, upTotal, downTotal);
        }

        public void iterativelyRun() throws Exception{
            if(semaphore.tryAcquire()){
                work();
                semaphore.release();
            }
        }
    }

    static class TrySemaphoreTimeOutThread extends SemaphoreTestThread{
        public TrySemaphoreTimeOutThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            super(semaphore, upTotal, downTotal);
        }

        public void iterativelyRun() throws Exception{
            if(semaphore.tryAcquire(1, TimeUnit.MILLISECONDS )){
                work();
                semaphore.release();
            }
        }
    }

    static abstract class SemaphoreTestThread extends Thread{
        static private final int MAX_ITTERATIONS = 1000*10;
        private final Random random = new Random();
        protected final ISemaphore semaphore ;
        protected final AtomicInteger upTotal;
        protected final AtomicInteger downTotal;
        public volatile Throwable error;

        public SemaphoreTestThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal){
            this.semaphore = semaphore;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        final public void run(){
            try{
                for ( int i=0; i<MAX_ITTERATIONS; i++ ) {
                    iterativelyRun();
                }
            }catch (Throwable e){
                error = e;
            }
        }

        abstract void iterativelyRun() throws Exception;

        protected void work(){
            final int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }
}
