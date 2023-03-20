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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setup() {
        factory.newHazelcastInstance(smallInstanceConfig());
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testIsUnisocketClient_whenTpcDisabledAndSmartRoutingDisabled() {
        verifyIsUnisocketClient(false, false);
    }

    @Test
    public void testIsUnisocketClient_whenTpcEnabledAndSmartRoutingDisabled() {
        verifyIsUnisocketClient(true, false);
    }

    @Test
    public void testIsUnisocketClient_whenTpcDisabledAndSmartRoutingEnabled() {
        verifyIsUnisocketClient(false, true);
    }

    @Test
    public void testIsUnisocketClient_whenTpcEnabledAndSmartRoutingEnabled() {
        verifyIsUnisocketClient(true, true);
    }

    private void verifyIsUnisocketClient(boolean tpcEnabled, boolean smartRouting) {
        ClientConfig config = new ClientConfig();
        config.getTpcConfig().setEnabled(tpcEnabled);
        config.getNetworkConfig().setSmartRouting(smartRouting);

        HazelcastInstance client = factory.newHazelcastClient(config);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);

        boolean isUnisocket = clientImpl.getConnectionManager().isUnisocketClient();
        // should be unisocket only when smart routing is false and TPC disabled
        assertEquals(!smartRouting && !tpcEnabled, isUnisocket);
    }
}
