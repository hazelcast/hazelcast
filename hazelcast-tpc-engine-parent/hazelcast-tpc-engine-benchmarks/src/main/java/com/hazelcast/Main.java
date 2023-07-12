package com.hazelcast;

public class Main {

    public static void main(String[] args) {
        for (int i = -20; i <= 20; i++) {
            double weight = 1024 / Math.pow(1.25, i);
            System.out.println("nice:" + i + " weight:" + weight + " weight corrected "+weight/1024);
        }
    }
}
