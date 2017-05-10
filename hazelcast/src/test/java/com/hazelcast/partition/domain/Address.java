package com.hazelcast.partition.domain;


import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;

public class Address implements Serializable {

    public static Random random = new Random();

    public String id;
    public int number;
    public String address;
    public String postCode;
    public String comments;

    public Address(){
        init();
    }

    public void init(){
        id = generateRandomString(8);
        number = random.nextInt();
        address = generateRandomString(48);
        postCode = generateRandomString(6);
        comments = generateRandomString(128);
    }

    @Override
    public String toString() {
        return "Address{" +
                "id='" + id + '\'' +
                ", number=" + number +
                ", address='" + address + '\'' +
                ", postCode='" + postCode + '\'' +
                ", comments='" + comments + '\'' +
                '}';
    }
}
