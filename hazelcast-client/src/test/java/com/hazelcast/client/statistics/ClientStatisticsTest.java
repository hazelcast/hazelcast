/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.statistics;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.statistics.Statistics;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientStatisticsTest extends ClientTestSupport {

    private static final int STATS_PERIOD_SECONDS = 1;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testNoUpdateWhenDisabled() {
        final HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();
        final ClientEngineImpl clientEngine = getClientEngineImpl(hazelcastInstance);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(Statistics.ENABLED.getName(), "false");
        clientConfig.setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS));

        hazelcastFactory.newHazelcastClient(clientConfig);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                Collection<Client> clients = clientEngine.getClients();
                if (!clients.isEmpty()) {
                    ClientEndpoint client = (ClientEndpoint) clients.iterator().next();
                    assertNull(client.getClientStatistics());
                }
            }
        }, STATS_PERIOD_SECONDS * 3);
    }

}
