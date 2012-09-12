package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ListenerManagerTest {
    private HazelcastInstance hc;

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        hc = Hazelcast.newHazelcastInstance(new Config());
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    /**
     * We want to test that the topic MessageListener is removed by object equality, not object identity, which is why we create two instances that are equal.
     */
    @Test
    public void testRemoveListener() throws Exception {
        final String topicName = "testTopic";
        final AtomicInteger counter = new AtomicInteger(0);
        IncrementingMessageListener listener1 = new IncrementingMessageListener(counter);
        hc.getTopic(topicName).addMessageListener(listener1);
        hc.getTopic(topicName).publish(new TestMessage());
        Thread.sleep(1000); // Wait for the message to be handled
        TestCase.assertEquals("A single increment is expected", 1, counter.get());
        IncrementingMessageListener listener2 = new IncrementingMessageListener(counter);
        TestCase.assertEquals("The IncrementingMessageListener classes should be equal!", listener1, listener2);
        hc.getTopic(topicName).removeMessageListener(listener2);
        hc.getTopic(topicName).publish(new TestMessage());
        Thread.sleep(1000); // Wait for the message to be handled
        TestCase.assertEquals("No additional increment is expected as we should remove the listeners by object equality, not object identity.", 1, counter.get());
    }
}

class TestMessage implements Serializable { }

/**
 * This delegates the equals and hashCode methods to the delegate AtomicInteger.
 */
class IncrementingMessageListener implements MessageListener<Object> {
    private final AtomicInteger counter;

    public IncrementingMessageListener(AtomicInteger counter) {
        this.counter = counter;
    }

    public void onMessage(Message<Object> message) {
        counter.incrementAndGet();
    }

    public AtomicInteger getCounter() {
        return counter;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof IncrementingMessageListener) {
            return counter.equals(((IncrementingMessageListener) obj).counter);
        }
        return false;
    }

    public int hashCode() {
        return counter.hashCode();
    }
}