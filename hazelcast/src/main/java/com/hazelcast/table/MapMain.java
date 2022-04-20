package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class MapMain {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        int items = 100_000_000;

        for (int k = 0; k < items; k++) {
            if (k % 10000 == 0) {
                System.out.println("Inserting at: " + k);
            }

            byte[] key = ("" + k).getBytes();
            byte[] value = new byte[1024];
            table.set(key, value);
        }
//
//        for (int k = 0; k < items; k++) {
//            if (k % 10000 == 0) {
//                System.out.println("Getting at: " + k);
//            }
//
//            String key = "" + k;
//            byte[] value = table.get(key.getBytes());
////            String actual = new String(value);
////            String expected = "value-" + k;
////            if (!expected.equals(actual)) {
////                throw new RuntimeException("Expected " + expected + " but found: " + actual);
////            }
//        }

        System.out.println("Done");
    }
}
