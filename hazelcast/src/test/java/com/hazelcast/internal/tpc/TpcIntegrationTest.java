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

package com.hazelcast.internal.tpc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.tpc.TpcServerBootstrap.TPC_ENABLED;
import static com.hazelcast.internal.tpc.TpcServerBootstrap.TPC_EVENTLOOP_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class TpcIntegrationTest extends HazelcastTestSupport {
    private static final int COUNT = 100_000;
    private static final int PRINT_PROGRESS_TIMES = 100;

    private final ILogger logger = Logger.getLogger(getClass());

    private HazelcastInstance server;
    private HazelcastInstance client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }

        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testMap() {
        System.setProperty(TPC_ENABLED.getName(), "true");
        System.setProperty(TPC_EVENTLOOP_COUNT.getName(), "" + Runtime.getRuntime().availableProcessors());
        server = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getTpcConfig().setEnabled(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        logger.info(">> Client created");
        IMap<Integer, Integer> map = client.getMap("foo");

        long startTime = System.currentTimeMillis();

        for (int k = 0; k < COUNT; k++) {
            if (k % (COUNT / PRINT_PROGRESS_TIMES) == 0) {
                logger.info(">> At:" + k);
            }
            map.put(k, k);
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = COUNT * 1000f / duration;
        logger.info(">> Throughput:" + throughput + " op/s");

        assertEquals(COUNT, map.size());
    }
}
