package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.concurrent.atomiclong.AtomicLongProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AtomicLongTest {

    public static volatile boolean stop = false;

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        //HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        new StressThread(hz1, "stressthread1").start();
        //new StressThread(hz2, "stressthread2").start();

        Thread.sleep(60 * 1000);
        System.exit(0);
    }

    public static class StressThread extends Thread {
        private final HazelcastInstance hz;
        private final List<IAtomicLong> counters = new ArrayList<IAtomicLong>();

        public StressThread(HazelcastInstance hz, String name) {
            super(name);
            this.hz = hz;

            Random random = new Random(0);

            for (int k = 0; k < 1; k++) {
                IAtomicLong atomicLong = hz.getAtomicLong("" + random.nextInt());
                counters.add(atomicLong);
            }
        }

        public void run() {
            long startMs = System.currentTimeMillis();
            long k = 0;
            try {

                while (!stop) {
                    for (IAtomicLong counter : counters) {
                        //long startNs = System.nanoTime();
                        counter.get();
                        //long durationNs = System.nanoTime() - startNs;
                        //if (durationNs> TimeUnit.SECONDS.toNanos(1)) {
                        //    AtomicLongProxy proxy = (AtomicLongProxy) counter;
                        //    System.out.println("operation on partition " + proxy.getPartitionId() + " took : " + TimeUnit.NANOSECONDS.toMillis(durationNs) + " ms");
                       // }
                    }

                    if (k % 1000000 == 0) {
                        System.out.println(getName() + " at " + k);
                    }
                    k++;
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }

            long durationMs = System.currentTimeMillis()-startMs;
            double performance = (k * 1000d)/durationMs;
            System.out.println("Performance :"+performance+" op/second");
        }
    }
}
