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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.htable.HTable;
import com.hazelcast.htable.Pipeline;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.tpc.apps.MainUtil.findPartition;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;

@SuppressWarnings({"checkstyle:MagicNumber", "VisibilityModifier"})
public class PipelineHashTableMain {

    public static long rounds = 400 * 1000;
    public static int pipelineSize = 2;
    public static int hashtableSize = 100_000;
    public static byte[][] keys;
    public static int partitionCount = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty(ClusterProperty.TPC_ENABLED.getName(), "true");
        System.setProperty(ClusterProperty.TPC_EVENTLOOP_COUNT.getName(), "1");
        System.setProperty("hazelcast.partition.count", "" + partitionCount);// for maximum pressure on the partition

        keys = new byte[hashtableSize][];

        HazelcastInstance localNode = Hazelcast.newHazelcastInstance();
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        int targetPartitionId = findPartition(remoteNode);


        HTable table = localNode.getProxy(HTable.class, "sometable");

        generateData(targetPartitionId, table);

        runBenchmark(table);

        localNode.shutdown();
        remoteNode.shutdown();
        System.exit(0);
    }

    private static void runBenchmark(HTable table) {
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

    private static void generateData(int targetPartitionId, HTable table) {
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
