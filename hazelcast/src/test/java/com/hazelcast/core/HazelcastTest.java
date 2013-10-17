package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class HazelcastTest {

    @Test(expected = NullPointerException.class)
    public void getOrCreateHazelcastInstance_nullConfig() {
        Hazelcast.getOrCreateHazelcastInstance(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateHazelcastInstance_nullName() {
        Config config = new Config();
        Hazelcast.getOrCreateHazelcastInstance(config);
    }

    @Test
    public void getOrCreateHazelcastInstance_noneExisting() {
        Config config = new Config(uniqueName());
        config.getGroupConfig().setName(uniqueName());

        HazelcastInstance hz = Hazelcast.getOrCreateHazelcastInstance(config);

        assertNotNull(hz);
        assertEquals(config.getInstanceName(), hz.getName());
        assertSame(hz, Hazelcast.getHazelcastInstanceByName(config.getInstanceName()));
        hz.shutdown();
    }

    @Test
    public void getOrCreateHazelcastInstance_existing() {
        Config config = new Config(uniqueName());
        config.getGroupConfig().setName(uniqueName());

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.getOrCreateHazelcastInstance(config);

        assertSame(hz1, hz2);
        hz1.shutdown();
    }

    public String uniqueName() {
        return UUID.randomUUID().toString();
    }
}
