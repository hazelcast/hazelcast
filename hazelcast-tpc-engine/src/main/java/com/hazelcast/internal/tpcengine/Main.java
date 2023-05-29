package com.hazelcast.internal.tpcengine;

public class Main {

    public static void main(String[] args) {
        double lastWeight = -1;
        for (int nice = 0; nice < 20; nice++) {
            double weight = 1f / Math.pow(1.25, nice);
            if (lastWeight == -1) {
                System.out.println("nice:" + nice + " weight:" + weight);
            } else {
                System.out.println("nice:" + nice + " weight:" + weight + " diff:" + ((weight - lastWeight) / lastWeight));
            }
            lastWeight = weight;
        }
    }
}
