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

package com.hazelcast.config;

import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NetworkConfigTest extends HazelcastTestSupport {

    private NetworkConfig networkConfig = new NetworkConfig();

    @Test
    public void testPort() throws Exception {
        int port = RandomPicker.getInt(0, 65536);
        networkConfig.setPort(port);
        assertEquals(port, networkConfig.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePort() throws Exception {
        int port = -1;
        networkConfig.setPort(port);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOverLimitPort() throws Exception {
        int port = 65536;
        networkConfig.setPort(port);
    }

    @Test
    public void testPortCount() throws Exception {
        int portCount = 111;
        networkConfig.setPortCount(portCount);
        assertEquals(portCount, networkConfig.getPortCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePortCount() throws Exception {
        int portCount = -1;
        networkConfig.setPortCount(portCount);
    }

    @Test
    public void testPortAutoIncrement() throws Exception {
        networkConfig.setPortAutoIncrement(true);
        assertTrue(networkConfig.isPortAutoIncrement());

        networkConfig.setPortAutoIncrement(false);
        assertFalse(networkConfig.isPortAutoIncrement());
    }

    @Test
    public void testReuseAddress() throws Exception {
        networkConfig.setReuseAddress(true);
        assertTrue(networkConfig.isReuseAddress());

        networkConfig.setReuseAddress(false);
        assertFalse(networkConfig.isReuseAddress());
    }

    @Test
    public void testPublicAddress() throws Exception {
        String publicAddress = "hazelcast.org";
        networkConfig.setPublicAddress(publicAddress);
        assertEquals(publicAddress, networkConfig.getPublicAddress());
    }

    @Test
    public void testRestApiConfig_isNotNullByDefault() {
        assertNotNull(networkConfig.getRestApiConfig());
    }

    @Test
    public void testMemcacheProtocolConfig_isNotNullByDefault() {
        assertNotNull(networkConfig.getMemcacheProtocolConfig());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(NetworkConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
