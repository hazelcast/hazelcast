package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.bootstrap.TpcServerBootstrap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AltoSocketConfigTest extends HazelcastTestSupport {
    private static final int EVENTLOOP_COUNT = 4;
    private final Config config = smallInstanceConfig();

    @Before
    public void setUp() throws Exception {
        config.getAltoConfig().setEnabled(true).setEventloopCount(EVENTLOOP_COUNT);
    }

    @Test
    public void testReceiveSize() {
        config.getNetworkConfig().getAltoSocketConfig().setReceiveBufferSize(1 << 20);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1 << 20, getTpcServerBootstrap(hz).getClientSocketConfig().getReceiveBufferSize());
    }

    @Test
    public void testSendSize() {
        config.getNetworkConfig().getAltoSocketConfig().setSendBufferSize(1 << 20);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1 << 20, getTpcServerBootstrap(hz).getClientSocketConfig().getSendBufferSize());
    }

    @Test
    public void testClientPortDefaults() {
        HazelcastInstance hz = createHazelcastInstance(config);
        ArrayList<Integer> expectedPorts = new ArrayList<>();
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(11000 + i);
        }
        assertEquals(expectedPorts, getTpcServerBootstrap(hz).getClientPorts());
    }

    @Test
    public void testClientPorts() {
        config.getNetworkConfig().getAltoSocketConfig().setPortRange("13000-14000");
        HazelcastInstance hz = createHazelcastInstance(config);
        ArrayList<Integer> expectedPorts = new ArrayList<>();
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(13000 + i);
        }
        assertEquals(expectedPorts, getTpcServerBootstrap(hz).getClientPorts());
    }

    @Test
    public void testClientPortsNotEnough() {
        config.getNetworkConfig().getAltoSocketConfig().setPortRange("13000-" + (13000 + EVENTLOOP_COUNT - 2));
        assertThrows(HazelcastException.class, () -> createHazelcastInstance(config));
    }

    @Test
    public void testClientPortsWith3Members() {
        config.getNetworkConfig().getAltoSocketConfig().setPortRange("13000-14000");
        HazelcastInstance[] hz = createHazelcastInstances(config, 3);
        ArrayList<Integer> expectedPorts = new ArrayList<>();

        // ports of hz0
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(13000 + i);
        }
        assertEquals(expectedPorts, getTpcServerBootstrap(hz[0]).getClientPorts());

        // ports of hz1
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.set(i, expectedPorts.get(i) + EVENTLOOP_COUNT);
        }
        assertEquals(expectedPorts, getTpcServerBootstrap(hz[1]).getClientPorts());

        // ports of hz2
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.set(i, expectedPorts.get(i) + EVENTLOOP_COUNT);
        }
        assertEquals(expectedPorts, getTpcServerBootstrap(hz[2]).getClientPorts());
    }

    @Test
    public void testConfigBounds() {
        AltoSocketConfig altoSocketConfig = config.getNetworkConfig().getAltoSocketConfig();
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setReceiveBufferSize(1 << 15 - 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setReceiveBufferSize(1 << 30 + 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setSendBufferSize(1 << 15 - 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setSendBufferSize(1 << 30 + 1));

        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("alto 4ever"));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("5701"));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("123123-123124"));
    }

    private static TpcServerBootstrap getTpcServerBootstrap(HazelcastInstance hz) {
        return getNode(hz).getNodeEngine().getTpcServerBootstrap();
    }
}
