package com.hazelcast.map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.ExecutionService;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DangerousMapAsyncTest {

    @Test
    public void testAsync() throws InterruptedException {
        int noOfInstances = 3;
        int noOfOps = 1000000;
        int noOfThreads = 10;

        HazelcastInstance hazelcastInstance = null;
        for (int i = 0; i < noOfInstances; i++) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }
        IMap<Integer, Integer> map =  hazelcastInstance.getMap("myMap");

        Inserter inserter = new Inserter(map, noOfOps);
        ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads);
        for (int i = 0; i < noOfThreads; i++) {
            executorService.submit(inserter);
        }
        executorService.awaitTermination(10, TimeUnit.MINUTES);


        Hazelcast.shutdownAll();
    }

    private class Inserter implements Runnable {
        private final int noOfOps;
        private final IMap<Integer, Integer> map;

        public Inserter(IMap<Integer, Integer> map, int noOfOps) {
            this.noOfOps = noOfOps;
            this.map = map;
        }

        private void insert() {
            for (int i = 0; i < noOfOps; i++) {
                map.putAsync(i, i);
                if (i % 1000 == 0) {
                    System.out.println("Iteration no. "+i);
                }
            }
        }


        @Override
        public void run() {
            insert();
        }
    }
}
