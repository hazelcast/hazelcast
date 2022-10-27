package com.hazelcast.internal.tpc;


import org.junit.Test;

public abstract class Eventloop_Configuration_Test {

    public abstract Eventloop.Configuration create();

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenZero(){
        Eventloop.Configuration configuration = create();
        configuration.setConcurrentRunQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenNegative(){
        Eventloop.Configuration configuration = create();
        configuration.setConcurrentRunQueueCapacity(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenZero(){
        Eventloop.Configuration configuration = create();
        configuration.setLocalRunQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenNegative(){
        Eventloop.Configuration configuration = create();
        configuration.setLocalRunQueueCapacity(-1);
    }
}
