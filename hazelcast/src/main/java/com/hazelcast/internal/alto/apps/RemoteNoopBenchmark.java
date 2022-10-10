package com.hazelcast.internal.alto.apps;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

/**
 * There is great variability between the runs. I believe this is related to the amount of batching that happens at the
 * network level when concurrency level is set higher than 1.
 */
public class RemoteNoopBenchmark {
    public final static long operations = 1000000;
    public final static int concurrency = 1;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled", "true");
        System.setProperty("hazelcast.tpc.reactor.count", "1");
        HazelcastInstance localNode = Hazelcast.newHazelcastInstance();
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        System.out.println("Waiting for partition tables to settle");
        Thread.sleep(5000);
        System.out.println("Waiting for partition tables to settle: done");
        int partitionId = remoteNode.getPartitionService().getPartitions().iterator().next().getPartitionId();

        Table table = localNode.getTable("sometable");

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
        localNode.shutdown();
        remoteNode.shutdown();
    }
}
