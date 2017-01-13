package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockConfigTest {

    private LockConfig config = new LockConfig();

    @Test
    public void testConstructor_withName() {
        config = new LockConfig("foobar");

        assertEquals("foobar", config.getName());
    }

    @Test
    public void testConstructor_withLockConfig() {
        config.setName("myName");
        config.setQuorumName("myQuorum");

        LockConfig cloned = new LockConfig(config);

        assertEquals(config.getName(), cloned.getName());
        assertEquals(config.getQuorumName(), cloned.getQuorumName());
    }

    @Test
    public void testConstructor_withLockConfigAndOverriddenName() {
        config.setName("myName");
        config.setQuorumName("myQuorum");

        LockConfig cloned = new LockConfig("newName", config);

        assertEquals("newName", cloned.getName());
        assertNotEquals(config.getName(), cloned.getName());
        assertEquals(config.getQuorumName(), cloned.getQuorumName());
    }

    @Test
    public void testToString() {
        assertNotNull(config.toString());
        assertContains(config.toString(), "LockConfig");
    }
}
