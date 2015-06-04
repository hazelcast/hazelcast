package com.hazelcast.map;


import org.junit.Test;
import com.hazelcast.core.IMap;
import org.junit.runner.RunWith;

import java.util.concurrent.CyclicBarrier;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;

import java.util.concurrent.BrokenBarrierException;

import com.hazelcast.test.HazelcastParallelClassRunner;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapPutDestroyTest extends HazelcastTestSupport {
    @Test
    public void testMapPutDestroy() {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap(randomString());
        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread t1 = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            barrier.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (BrokenBarrierException e) {
                            e.printStackTrace();
                        }

                        for (int i = 0; i < 10000; i++) {
                            map.put(i, "Value" + i);
                        }
                    }
                }
        );

        Thread t2 = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            barrier.await();
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (BrokenBarrierException e) {
                            e.printStackTrace();
                        }

                        map.destroy();
                    }
                }
        );

        t1.start();
        t2.start();

        try {
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
