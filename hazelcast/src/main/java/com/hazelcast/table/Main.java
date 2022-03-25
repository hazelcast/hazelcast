package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Packet;

public class Main {

    public static void main(String[] args) {
//        Packet packet = new Packet(new byte[120], 0);
//
//        System.out.println(packet);
//
//        if(true){
//            return;
//        }

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        for (int k = 0; k < 20; k++) {
            Item item = new Item();
            item.key = 1;
            item.a = 2;
            item.b = 3;
            System.out.println("-----------------call: "+k);
            table.upsert(item);

        }

        System.out.println("Done");
    }
}
