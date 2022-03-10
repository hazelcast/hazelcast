package com.hazelcast.table;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        Table table = hazelcastInstance.getTable("foo");
        Item item = new Item();
        item.key = 1;
        item.a = 2;
        item.b = 3;
        table.upsert(item);
        System.out.println("Done");
    }
}
