package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.bootstrap.TpcServerBootstrap;
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
        assertFalse(isTpcEnabled(createHazelcastInstance()));
    }

    @Test
    public void testEventloopCountDefault() {
        config.getAltoConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(Runtime.getRuntime().availableProcessors(), getEventloopCount(hz));
    }

    @Test
    public void testEventloopCount() {
        config.getAltoConfig().setEnabled(true).setEventloopCount(7);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(7, getEventloopCount(hz));
    }

    private static int getEventloopCount(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getTpcEngine().eventloopCount();
    }

    private static boolean isTpcEnabled(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).isEnabled();
    }

    private static TpcServerBootstrap getTpcServerBootstrap(HazelcastInstance hz) {
        return getNode(hz).getNodeEngine().getTpcServerBootstrap();
    }
}
