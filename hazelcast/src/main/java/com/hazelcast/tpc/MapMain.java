package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class MapMain {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        int items = 1_000_000;

        for (int k = 0; k < items; k++) {
            if (k % 100000 == 0) {
                System.out.println("Inserting at: " + k);
            }

            byte[] key = ("" + k).getBytes();
            byte[] value = ("value-" + k).getBytes();
            //byte[] value = new byte[1024];
            table.set(key, value);
        }
//
        long start = System.currentTimeMillis();
        int queryCount = 2000;
        for (int k = 0; k < queryCount; k++) {
            if (k % 1000 == 0) {
                System.out.println("Getting at: " + k);
            }

            table.bogusQuery();
//
//            String key = "" + k;
//            byte[] value = table.get(key.getBytes());
//            String actual = new String(value);
//            String expected = "value-" + k;
//            if (!expected.equals(actual)) {
//                throw new RuntimeException("Expected " + expected + " but found: " + actual);
//            }
        }

        long duration = System.currentTimeMillis() - start;
        double throughput = queryCount * 1000f / duration;
        System.out.println("throughput: " + throughput);
        System.out.println("Done");
    }
}
