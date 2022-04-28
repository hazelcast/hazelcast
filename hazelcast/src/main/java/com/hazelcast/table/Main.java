package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("piranaha");

        for (int k = 0; k < 100_000_000; k++) {
            Item item = new Item();
            item.key = 1;
            item.a = 2;
            item.b = 3;

//            System.out.println("========================================================================");
//            System.out.println("k="+k);
//            System.out.println("========================================================================");

            if (k % 100 == 0) {
                System.out.println("at k:" + k);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

//            Pipeline pipeline = table.newPipeline();
//            for (int l = 0; l < 10; l++) {
//                pipeline.noop(0);
//            }
//            pipeline.execute();
//            pipeline.await();

            //table.upsert(item);
            table.concurrentNoop(1);


        }

        System.out.println("Done");
    }
}
