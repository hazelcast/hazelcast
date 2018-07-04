package com.hazelcast;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args){
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1,2,1000, TimeUnit.SECONDS,new SynchronousQueue<Runnable>());
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + " started");
                    Thread.sleep(10000);
                    System.out.println(Thread.currentThread().getName() + " started");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + " started");
                    Thread.sleep(10000);
                    System.out.println(Thread.currentThread().getName() + " started");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

    }
}
