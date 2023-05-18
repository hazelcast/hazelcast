/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.apps;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.htable.HTable;
import com.hazelcast.htable.Pipeline;
import com.hazelcast.spi.properties.ClusterProperty;

@SuppressWarnings({"checkstyle:MagicNumber", "VisibilityModifier"})
public class PipelineNoopMain {

    public static long rounds = 1000 * 1000;
    public static long pipelineSize = 10;
    public static int partitionCount = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty(ClusterProperty.TPC_ENABLED.getName(), "true");
        System.setProperty(ClusterProperty.TPC_EVENTLOOP_COUNT.getName(), "1");
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
