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
import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.tpc.apps.MainUtil.findPartition;

@SuppressWarnings({"checkstyle:MagicNumber", "VisibilityModifier"})
public class ClassicPipelining {

    public static long rounds = 10 * 1000;
    public static int pipelineSize = 8192;
    public static int hashtableSize = 100_000;
    public static byte[][] keys;
    public static int partitionCount = 10;
    private static long keyGenerator = 0;


    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.partition.count", "" + partitionCount);// for maximum pressure on the partition
        System.setProperty("hazelcast.operation.thread.count", "1");

        HazelcastInstance localNode = Hazelcast.newHazelcastInstance();
        HazelcastInstance remoteNode = Hazelcast.newHazelcastInstance();

        int targetPartitionId = findPartition(remoteNode);

        keys = new byte[hashtableSize][];
        IMap<byte[], byte[]> map = localNode.getMap("map");

        generateData(targetPartitionId, map, localNode);

        runBenchmark(keys, map);
        localNode.shutdown();
        remoteNode.shutdown();
        System.exit(0);
    }

    private static void generateData(int targetPartition, IMap<byte[], byte[]> map, HazelcastInstance hz) {
        System.out.println("Generating data");

        for (int k = 0; k < hashtableSize; k++) {
            byte[] key = generateKeyFor(targetPartition, hz);
            keys[k] = key;
            map.set(key, "fooo".getBytes());

            if (k % 1000 == 0) {
                System.out.println("\tinserting:" + k);
            }
        }

        System.out.println("Generating data:Done");
    }

    private static void runBenchmark(byte[][] keys, IMap<byte[], byte[]> map) throws Exception {
        long startMs = System.currentTimeMillis();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int round = 0; round < rounds; round++) {
            Pipelining pipeline = new Pipelining(pipelineSize);
            for (int l = 0; l < pipelineSize; l++) {
                byte[] key = keys[random.nextInt(keys.length)];
                pipeline.add(map.getAsync(key));
            }
            pipeline.results();

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

    private static byte[] generateKeyFor(int targetPartitionId, HazelcastInstance hz) {

        byte[] key = new byte[8];
        for (; ; ) {
            long value = keyGenerator;
            // copy the value into key
            for (int i = 7; i >= 0; i--) {
                key[i] = (byte) (value & 0xFF);
                value >>= 8;
            }

            Partition foundPartition = hz.getPartitionService().getPartition(key);
            if (foundPartition.getPartitionId() == targetPartitionId) {
                return key;
            }

            keyGenerator++;
        }
    }
}
