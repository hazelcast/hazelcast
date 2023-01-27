/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
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
        getAltoSocketConfig().setReceiveBufferSize(1 << 20);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1 << 20, getClientSocketConfig(hz).getReceiveBufferSize());
    }

    @Test
    public void testSendSize() {
        getAltoSocketConfig().setSendBufferSize(1 << 20);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1 << 20, getClientSocketConfig(hz).getSendBufferSize());
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
    public void testConfigBounds() {
        AltoSocketConfig altoSocketConfig = getAltoSocketConfig();
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setReceiveBufferSize(1 << 15 - 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setReceiveBufferSize(1 << 30 + 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setSendBufferSize(1 << 15 - 1));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setSendBufferSize(1 << 30 + 1));

        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("alto 4ever"));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("5701"));
        assertThrows(InvalidConfigurationException.class, () -> altoSocketConfig.setPortRange("123123-123124"));
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
