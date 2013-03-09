package com.hazelcast.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigTest {

    @Test
    public void test(){
        Config config = new Config();
        QueueConfig queueConfig = new QueueConfig().setName("somequeue");
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig, config.getQueueConfig("somequeue"));
        assertEquals(queueConfig, config.getQueueConfig("somequeue@foo"));
    }
}
