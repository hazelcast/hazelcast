package com.hazelcast.core;

public class Timer {
    private long start;
    private long end;
    private long numberOfOperations;

    public Timer(){
        this.start = System.currentTimeMillis();
    }

    public Timer(long numberOfOperations){
        this.start = System.currentTimeMillis();
        this.numberOfOperations = numberOfOperations;
    }

    public void stop(){
        this.end = System.currentTimeMillis();
    }

    public long elapsed(){
        return end - start;
    }

    public double operationsPerSecond(){
        return (double)numberOfOperations/((double)elapsed()/1000d);
    }

    public void printResult(String methodName){
        System.out.println(methodName);
        System.out.println("elapsed (ms): " + elapsed());
        System.out.println("operations/sec: " + operationsPerSecond());
    }
}
