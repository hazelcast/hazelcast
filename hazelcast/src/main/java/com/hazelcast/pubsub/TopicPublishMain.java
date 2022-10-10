package com.hazelcast.pubsub;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class TopicPublishMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled", "true");
        System.setProperty("hazelcast.tpc.eventloop.count", "1");
        System.setProperty("hazelcast.alto.eventloop.type", "io_uring");

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        // HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("sometable");

        long start = System.currentTimeMillis();
        int publishCount = 10 * 1000 * 1000;
        byte[] message = "foobardvdsgdsgfdsgfdsfsfdsfsdfdsfdsfsdfdsfaf".getBytes();
        Publisher publisher = table.createPublisher("sometopic");
        for (int k = 0; k < publishCount; k++) {
            if (k % 100000 == 0) {
                System.out.println("Getting at: " + k);
            }

            long sequenceNr = publisher.publish(0, message, Publisher.SYNC_NONE);
            //System.out.println(sequenceNr);
            //System.out.println(sequenceNr);
        }

        long duration = System.currentTimeMillis() - start;
        double throughput = publishCount * 1000f / duration;
        System.out.println("throughput: " + throughput);
        System.out.println("Done");
    }
}
