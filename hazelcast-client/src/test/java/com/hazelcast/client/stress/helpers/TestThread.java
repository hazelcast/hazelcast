package com.hazelcast.client.stress.helpers;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertNull;

public abstract class TestThread extends Thread {
    private final static AtomicLong ID_GENERATOR = new AtomicLong(1);
    public final Random random = new Random();
    public volatile Throwable error = null;

    public static volatile AtomicInteger stoped;

    private CountDownLatch startLatch;

    public HazelcastInstance instance;

    public TestThread(HazelcastInstance instance) {
        this.instance = instance;
        setName(getClass().getName() + ID_GENERATOR.getAndIncrement());
    }

    @Override
    public final void run() {
        try {
            startLatch.await();

            while(stoped.get()!=0){
                doRun();
            }

        } catch (Throwable t) {
            error = t;
            t.printStackTrace();
        }
    }

    public final void assertNoError() {
        assertNull(getName() + " encountered an error", error);
    }

    public abstract void doRun() throws Exception;

    public void setStartLatch(CountDownLatch startLatch){
        this.startLatch = startLatch;
    }

    public void setStoped(AtomicInteger stoped){
        this.stoped = stoped;
    }
}
