package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

// control plane data plane

public class LoadMain {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();


        Table table = node1.getTable("foo");

        int count = 1000  * 1000;
        int concurrency = 1;
        long start = System.currentTimeMillis();
        for (int k = 0; k < count / concurrency; k++) {
            //table.concurrentNoop(concurrency);
            table.concurrentRandomLoad("foo".getBytes(), concurrency);
        }
        long duration = System.currentTimeMillis() - start;

        System.out.println("throughput:" + (count * 1000f / duration));

        System.out.println("done");
    }
}
