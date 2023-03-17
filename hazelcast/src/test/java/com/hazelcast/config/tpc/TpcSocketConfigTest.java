/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.tpc;

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

import static com.hazelcast.config.tpc.TpcConfigAccessors.getClientPorts;
import static com.hazelcast.config.tpc.TpcConfigAccessors.getClientSocketConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TpcSocketConfigTest extends HazelcastTestSupport {
    private static final int EVENTLOOP_COUNT = 4;
    private final Config config = smallInstanceConfig();

    @Before
    public void setUp() throws Exception {
        config.getTpcConfig().setEnabled(true).setEventloopCount(EVENTLOOP_COUNT);
    }

    @Test
    public void testReceiveSize() {
        getTpcSocketConfig().setReceiveBufferSizeKB(1024);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1024, getClientSocketConfig(hz).getReceiveBufferSizeKB());
    }

    @Test
    public void testSendSize() {
        getTpcSocketConfig().setSendBufferSizeKB(1024);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertEquals(1024, getClientSocketConfig(hz).getSendBufferSizeKB());
    }

    @Test
    public void testClientPortDefaults() {
        HazelcastInstance hz = createHazelcastInstance(config);
        assertThat(getClientPorts(hz))
                .allSatisfy(i -> assertThat(i).isBetween(11000, 21000))
                .hasSize(EVENTLOOP_COUNT);
    }

    @Test
    public void testClientPorts() {
        getTpcSocketConfig().setPortRange("13000-14000");
        HazelcastInstance hz = createHazelcastInstance(config);
        assertThat(getClientPorts(hz))
                .allSatisfy(i -> assertThat(i).isBetween(13000, 14000))
                .hasSize(EVENTLOOP_COUNT);
    }

    @Test
    public void testClientPortsNotEnough() {
        getTpcSocketConfig().setPortRange("13000-" + (13000 + EVENTLOOP_COUNT - 2));
        assertThrows(HazelcastException.class, () -> createHazelcastInstance(config));
    }

    @Test
    public void testCreatingHazelcastInstanceThrows_whenPortRangeMatchesButDecreasing() {
        // 13000-12000 will match the regex in setPortRange() but should throw
        getTpcSocketConfig().setPortRange("13000-12000");
        assertThrows(HazelcastException.class, () -> createHazelcastInstance(config));
    }

    @Test
    public void testClientPortsWith3Members() {
        getTpcSocketConfig().setPortRange("13000-14000");
        HazelcastInstance[] hz = createHazelcastInstances(config, 3);
        assertThat(hz).allSatisfy(
                instance -> assertThat(getClientPorts(instance))
                        .allSatisfy(i -> assertThat(i).isBetween(13000, 14000))
                        .hasSize(EVENTLOOP_COUNT));
    }

    @Test
    public void testConfigValidation() {
        TpcSocketConfig tpcSocketConfig = getTpcSocketConfig();
        assertThrows(IllegalArgumentException.class, () -> tpcSocketConfig.setReceiveBufferSizeKB(0));
        assertThrows(IllegalArgumentException.class, () -> tpcSocketConfig.setSendBufferSizeKB(0));

        assertThrows(IllegalArgumentException.class, () -> tpcSocketConfig.setPortRange("tpc 4ever"));
        assertThrows(IllegalArgumentException.class, () -> tpcSocketConfig.setPortRange("5701"));
        assertThrows(IllegalArgumentException.class, () -> tpcSocketConfig.setPortRange("123123-123124"));
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(TpcConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    private TpcSocketConfig getTpcSocketConfig() {
        return config.getNetworkConfig().getTpcSocketConfig();
    }
}
