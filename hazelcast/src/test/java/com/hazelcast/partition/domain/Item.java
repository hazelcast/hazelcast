package com.hazelcast.partition.domain;

import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;

public class Item implements Serializable {

    public static Random rand = new Random();

    public String id;
    public String name;
    public double price;

    public Item(){
        init();
    }

    public void init(){
        id = generateRandomString(8);
        name = generateRandomString(10);
        price = rand.nextDouble();
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}
