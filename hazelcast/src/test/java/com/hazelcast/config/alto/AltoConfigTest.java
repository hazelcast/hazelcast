package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AltoConfigTest extends HazelcastTestSupport {
    private final Config config = smallInstanceConfig();

    @Test
    public void testTPCDisabledByDefault() {
        HazelcastInstance hz = createHazelcastInstance();
        assertFalse(getNode(hz).getNodeEngine().getTpcServerBootstrap().isEnabled());
    }

    @Test
    public void testEventloopCountDefault() {
        config.getAltoConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(getNode(hz).getNodeEngine().getTpcServerBootstrap().isEnabled());
        assertEquals(
                Runtime.getRuntime().availableProcessors(),
                getNode(hz).getNodeEngine().getTpcServerBootstrap().getTpcEngine().eventloopCount());
    }

    @Test
    public void testEventloopCount() {
        config.getAltoConfig().setEnabled(true);
        config.getAltoConfig().setEventloopCount(7);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(getNode(hz).getNodeEngine().getTpcServerBootstrap().isEnabled());
        assertEquals(
                7,
                getNode(hz).getNodeEngine().getTpcServerBootstrap().getTpcEngine().eventloopCount());
    }
}
