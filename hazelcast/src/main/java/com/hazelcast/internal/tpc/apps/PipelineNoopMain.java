
package com.hazelcast.internal.tpc.apps;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.htable.Pipeline;
import com.hazelcast.htable.HTable;
import com.hazelcast.spi.properties.ClusterProperty;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

public class PipelineNoopMain {

    public static long rounds = 1000 * 1000;
    public static long pipelineSize = 10;
    public static int partitionCount = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty(ClusterProperty.TPC_ENABLED.getName(),"true");
        System.setProperty(ClusterProperty.TPC_EVENTLOOP_COUNT.getName(),"1");
        System.setProperty("hazelcast.partition.count", "" + partitionCount);// for maximum pressure on the partition

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HazelcastInstance localNode = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        System.out.println("Waiting for partition tables to settle");
        Thread.sleep(5000);
        System.out.println("Waiting for partition tables to settle: done");
        int targetPartitionId = remoteNode.getPartitionService().getPartitions().iterator().next().getPartitionId();

        HTable table = localNode.getProxy(HTable.class, "sometable");

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
