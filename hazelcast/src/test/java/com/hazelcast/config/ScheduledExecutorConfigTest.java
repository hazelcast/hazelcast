package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorConfigTest {

    private ScheduledExecutorConfig config = new ScheduledExecutorConfig();

    @Test
    public void testConstructor_withName() {
        config = new ScheduledExecutorConfig("myName");

        assertEquals("myName", config.getName());
    }

    @Test
    public void testName() {
        config.setName("myName");

        assertEquals("myName", config.getName());
    }

    @Test
    public void testPoolSize() {
        config.setPoolSize(23);

        assertEquals(23, config.getPoolSize());
    }

    @Test
    public void testDurability()  {
        config.setDurability(42);

        assertEquals(42, config.getDurability());
    }

    @Test
    public void testToString() {
        assertTrue(config.toString().contains("ScheduledExecutorConfig"));
    }
}
