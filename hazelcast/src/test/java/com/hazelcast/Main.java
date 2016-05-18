package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.properties.GroupProperty;

/**
 * Created by alarmnummer on 5/18/16.
 */
public class Main {

    public static void main(String[] args) {
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALLER_RUNS.getName(), "true");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
       // HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);


        IAtomicLong[] refs = new IAtomicLong[1000];
        for(int k=0;k<refs.length;k++){
            refs[k]=hz1.getAtomicLong(""+k);
        }

        long startMs = System.currentTimeMillis();
        long iterations = 100 * 1000 * 1000;
        for (long k = 0; k < iterations; k++) {
            refs[(int)(k%refs.length)].get();

            if (k % 1000000 == 0) {
                System.out.println("at:" + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double throughput = (1000d * iterations) / durationMs;
        System.out.println("Throughput: " + throughput + " op/second");
        System.exit(0);
    }

    static class MyFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long inpunt) {
            System.out.println(Thread.currentThread());
            return 10l;
        }
    }
}
