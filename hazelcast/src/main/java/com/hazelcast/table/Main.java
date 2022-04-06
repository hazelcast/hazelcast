package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Packet;

public class Main {

    public static void main(String[] args) {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
         HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        for (int k = 0; k < 1000; k++) {
            Item item = new Item();
            item.key = 1;
            item.a = 2;
            item.b = 3;

//            System.out.println("========================================================================");
//            System.out.println("k="+k);
//            System.out.println("========================================================================");

            if(k%1000==0){
                System.out.println("at k:"+k);
            }

            //table.upsert(item);
            table.concurrentNoop(100);
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

        }

        System.out.println("Done");
    }
}
