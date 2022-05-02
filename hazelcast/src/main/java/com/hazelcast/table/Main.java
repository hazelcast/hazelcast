package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        long operations = 50_000_000;
        int concurrency = 100;
        long iterations = operations/concurrency;

        long startMs = System.currentTimeMillis();
        long count = 0;
        for (int k = 0; k < iterations; k++) {

            if (count % 1_000_000 == 0) {
                System.out.println("at k:" + count);
            }

            table.concurrentNoop(concurrency);
            count += concurrency;
        }

        System.out.println("Done");

        long duration = System.currentTimeMillis()-startMs;
        System.out.println("Throughput: "+(operations*1000.0f/duration)+" op/s");
    }
}
