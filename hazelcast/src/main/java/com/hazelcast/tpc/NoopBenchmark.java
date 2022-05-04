package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class NoopBenchmark {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        long operations = 5_000_000;
        int concurrency = 2;
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
        node1.shutdown();
        node2.shutdown();
    }
}
