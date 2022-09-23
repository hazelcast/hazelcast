package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class RemoteNoopBenchmark {

    public static void main(String[] args) throws Exception {
        System.setProperty("reactor.count","1");
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        System.out.println("Waiting for partition tables to settle");
        Thread.sleep(5000);
        System.out.println("Waiting for partition tables to settle: done");
        int partitionId = node2.getPartitionService().getPartitions().iterator().next().getPartitionId();

        Table table = node1.getTable("piranaha");

        long operations = 2_000_000;
        int concurrency = 1;
        long iterations = operations / concurrency;

        long startMs = System.currentTimeMillis();
        long count = 0;
        for (int k = 0; k < iterations; k++) {

            if (count % 100_000 == 0) {
                System.out.println("at k:" + count);
            }

            table.concurrentNoop(concurrency, partitionId);
            count += concurrency;
        }

        System.out.println("Done");

        long duration = System.currentTimeMillis() - startMs;
        System.out.println("Throughput: " + (operations * 1000.0f / duration) + " op/s");
        node1.shutdown();
    }
}
