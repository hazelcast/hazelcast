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

package com.hazelcast.pubsub;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.htable.HTable;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Random;

@SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:HideUtilityClassConstructor", "checkstyle:ConstantName"})
public class TopicPublishMain {

    public static void main(String[] args) throws Exception {
        System.setProperty(ClusterProperty.TPC_ENABLED.getName(), "true");
        System.setProperty(ClusterProperty.TPC_EVENTLOOP_COUNT.getName(), "1");
        System.setProperty("hazelcast.partition.count", "20");
        System.setProperty("hazelcast.tpc.eventloop.type", "io_uring");

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        // HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        HTable table = node1.getProxy(HTable.class, "sometable");

        long start = System.currentTimeMillis();
        int iterations = 200 * 1000 * 1000;

        Random random = new Random();
        Publisher publisher = node1.getProxy(Publisher.class, "sometopic");
        int messageSize = 8024;
        int maxMessageSize = 65586;
        long bytesProduced = 0;
        for (int k = 0; k < iterations; k++) {
            int m = messageSize == -1
                    ? random.nextInt(maxMessageSize)
                    : messageSize;
            bytesProduced += m;
            byte[] message = new byte[m];
            random.nextBytes(message);

            if (k % 100000 == 0) {
                System.out.println("Getting at: " + k);
            }

            publisher.publish(message, Publisher.SYNC_NONE);
            //System.out.println(sequenceNr);
            //System.out.println(sequenceNr);
        }

        long duration = System.currentTimeMillis() - start;
        double throughput = iterations * 1000f / duration;
        double mbPerSecond = (bytesProduced * 1000f) / (1024 * 1024 * duration);
        System.out.println("throughput: " + throughput);
        System.out.println(mbPerSecond + " mb/second: ");
        System.out.println("Done");
        node1.shutdown();
    }
}
