package com.hazelcast.util;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Test basic lock operation of {@link MutexProvider}
 */
@Category({QuickTest.class})
public class MutexProviderTest {

    private MutexProvider mutexProvider;
    private final AtomicBoolean testFailed = new AtomicBoolean(false);


    @Before
    public void setup() {
        mutexProvider = new MutexProvider();
    }

    @Test
    public void testConcurrentMutexOperation() {
        final String[] keys = new String[] {"a", "b", "c"};
        final Map<String, Integer> timesAcquired = new HashMap<String, Integer>();

        int concurrency = Runtime.getRuntime().availableProcessors()*3;
        final CyclicBarrier cyc = new CyclicBarrier(concurrency+1);

        for (int i=0; i<concurrency; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    await(cyc);

                    for (String key : keys) {
                        MutexProvider.Mutex mutex = mutexProvider.getMutex(key);
                        try {
                            synchronized (mutex) {
                                Integer value = timesAcquired.get(key);
                                if (value == null) {
                                    timesAcquired.put(key, 1);
                                } else {
                                    timesAcquired.put(key, value + 1);
                                }
                            }
                        }
                        finally {
                            mutex.close();
                        }
                    }

                    await(cyc);
                }
            }).start();
        }
        // start threads, wait for them to finish
        await(cyc);
        await(cyc);

        // assert each key's lock was acquired by each thread
        for (String key : keys) {
            assertEquals(concurrency, timesAcquired.get(key).longValue());
        }
        // assert there are no mutexes leftover
        assertEquals(0, mutexProvider.mutexMap.size());
    }

    private void await(CyclicBarrier cyc) {
        try {
            cyc.await();
        } catch (InterruptedException e) {
            testFailed.set(true);
        } catch (BrokenBarrierException e) {
            testFailed.set(true);
        }
    }
}
