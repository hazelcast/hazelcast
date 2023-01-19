package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static com.hazelcast.config.alto.AltoConfigAccessors.getClientPorts;
import static com.hazelcast.config.alto.AltoConfigAccessors.getClientSocketConfig;
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
        getAltoSocketConfig().setReceiveBufferSizeKB(1024);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1024, getClientSocketConfig(hz).getReceiveBufferSizeKB());
    }

    @Test
    public void testSendSize() {
        getAltoSocketConfig().setSendBufferSizeKB(1024);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1024, getClientSocketConfig(hz).getSendBufferSizeKB());
    }

    @Test
    public void testClientPortDefaults() {
        HazelcastInstance hz = createHazelcastInstance(config);
        ArrayList<Integer> expectedPorts = new ArrayList<>();
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(11000 + i);
        }
        assertEquals(expectedPorts, getClientPorts(hz));
    }

    @Test
    public void testClientPorts() {
        getAltoSocketConfig().setPortRange("13000-14000");
        HazelcastInstance hz = createHazelcastInstance(config);
        ArrayList<Integer> expectedPorts = new ArrayList<>();
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(13000 + i);
        }
        assertEquals(expectedPorts, getClientPorts(hz));
    }

    @Test
    public void testClientPortsNotEnough() {
        getAltoSocketConfig().setPortRange("13000-" + (13000 + EVENTLOOP_COUNT - 2));
        assertThrows(HazelcastException.class, () -> createHazelcastInstance(config));
    }

    @Test
    public void testCreatingHazelcastInstanceThrows_whenPortRangeMatchesButDecreasing() {
        // 13000-12000 will match the regex in setPortRange() but should throw
        getAltoSocketConfig().setPortRange("13000-12000");
        assertThrows(HazelcastException.class, () -> createHazelcastInstance(config));
    }

    @Test
    public void testClientPortsWith3Members() {
        getAltoSocketConfig().setPortRange("13000-14000");
        HazelcastInstance[] hz = createHazelcastInstances(config, 3);
        ArrayList<Integer> expectedPorts = new ArrayList<>();

        // ports of hz0
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.add(13000 + i);
        }
        assertEquals(expectedPorts, getClientPorts(hz[0]));

        // ports of hz1
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.set(i, expectedPorts.get(i) + EVENTLOOP_COUNT);
        }
        assertEquals(expectedPorts, getClientPorts(hz[1]));

        // ports of hz2
        for (int i = 0; i < EVENTLOOP_COUNT; i++) {
            expectedPorts.set(i, expectedPorts.get(i) + EVENTLOOP_COUNT);
        }
        assertEquals(expectedPorts, getClientPorts(hz[2]));
    }

    @Test
    public void testConfigValidation() {
        AltoSocketConfig altoSocketConfig = getAltoSocketConfig();
        assertThrows(IllegalArgumentException.class, () -> altoSocketConfig.setReceiveBufferSizeKB(0));
        assertThrows(IllegalArgumentException.class, () -> altoSocketConfig.setSendBufferSizeKB(0));

        assertThrows(IllegalArgumentException.class, () -> altoSocketConfig.setPortRange("alto 4ever"));
        assertThrows(IllegalArgumentException.class, () -> altoSocketConfig.setPortRange("5701"));
        assertThrows(IllegalArgumentException.class, () -> altoSocketConfig.setPortRange("123123-123124"));
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(AltoConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    private AltoSocketConfig getAltoSocketConfig() {
        return config.getNetworkConfig().getAltoSocketConfig();
    }
}
