
package com.hazelcast.internal.alto.apps;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Pipeline;
import com.hazelcast.table.Table;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

public class PipelineNoopMain {

    public static long rounds = 1000 * 1000;
    public static long pipelineSize = 10;
    public static int partitionCount = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled", "true");
        System.setProperty("hazelcast.partition.count", "" + partitionCount);// for maximum pressure on the partition
        System.setProperty("hazelcast.tpc.reactor.count", "1");

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HazelcastInstance localNode = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        System.out.println("Waiting for partition tables to settle");
        Thread.sleep(5000);
        System.out.println("Waiting for partition tables to settle: done");
        int targetPartitionId = remoteNode.getPartitionService().getPartitions().iterator().next().getPartitionId();

        Table table = localNode.getTable("sometable");

        long startMs = System.currentTimeMillis();
        Pipeline pipeline = table.newPipeline();
        for (int round = 0; round < rounds; round++) {
            for (int l = 0; l < pipelineSize; l++) {
                pipeline.noop(targetPartitionId);
            }
            pipeline.execute();
            pipeline.reset();

            if (round % 100000 == 0) {
                System.out.println("at round:" + round);
            }
        }

        System.out.println("Done");

        long duration = System.currentTimeMillis() - startMs;
        System.out.println("Pipeline: " + (rounds * 1000.0f / duration) + " op/s");
        System.out.println("Pipelined operations: " + (rounds * pipelineSize * 1000.0f / duration) + " op/s");

        remoteNode.shutdown();
        localNode.shutdown();
    }
}
