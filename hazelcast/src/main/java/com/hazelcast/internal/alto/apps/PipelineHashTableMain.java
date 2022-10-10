
package com.hazelcast.internal.alto.apps;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Pipeline;
import com.hazelcast.table.Table;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.alto.apps.MainUtil.findPartition;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;

public class PipelineHashTableMain {

    public static long rounds = 400 * 1000;
    public static int pipelineSize = 2;
    public static int hashtableSize = 100_000;
    public static byte[][] keys;
    public static int partitionCount = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled", "true");
        System.setProperty("hazelcast.partition.count", "" + partitionCount);// for maximum pressure on the partition
        System.setProperty("hazelcast.tpc.reactor.count", "1");

        keys = new byte[hashtableSize][];

        HazelcastInstance localNode = Hazelcast.newHazelcastInstance();
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        int targetPartitionId = findPartition(remoteNode);


        Table table = localNode.getTable("sometable");

        generateData(targetPartitionId, table);

        runBenchmark(table);

        localNode.shutdown();
        remoteNode.shutdown();
        System.exit(0);
    }

    private static void runBenchmark(Table table) {
        long startMs = System.currentTimeMillis();
        Pipeline pipeline = table.newPipeline();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int round = 0; round < rounds; round++) {
            for (int l = 0; l < pipelineSize; l++) {
                pipeline.get(keys[random.nextInt(keys.length)]);
            }
            pipeline.execute();
            pipeline.reset();

            if (round % 10000 == 0) {
                System.out.println("at round:" + round);
            }
        }

        System.out.println("Done");
        long duration = System.currentTimeMillis() - startMs;
        System.out.println("Duration: " + duration + " ms");
        System.out.println("Pipeline: " + (rounds * 1000.0f / duration) + " op/s");
        System.out.println("Pipelined operations: " + (rounds * pipelineSize * 1000.0f / duration) + " op/s");
    }

    private static void generateData(int targetPartitionId, Table table) {
        System.out.println("Generating data");

        for (int k = 0; k < hashtableSize; k++) {
            byte[] key = generateKeyFor(targetPartitionId);
            keys[k] = key;
            table.set(key, "fooo".getBytes());

            if (k % 100 == 0) {
                System.out.println("\tinserting:" + k);
            }
        }

        System.out.println("Generating data: Done");
    }

    static long keyGenerator = 0;

    private static byte[] generateKeyFor(int targetPartitionId) {
        byte[] key = new byte[8];
        for (; ; ) {
            long value = keyGenerator;
            // copy the value into key
            for (int i = 7; i >= 0; i--) {
                key[i] = (byte) (value & 0xFF);
                value >>= 8;
            }

            int foundPartitionId = hashToIndex(Arrays.hashCode(key), partitionCount);
            if (foundPartitionId == targetPartitionId) {
                return key;
            }
            keyGenerator++;
        }
    }
}
