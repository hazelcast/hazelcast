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

package com.hazelcast.internal.serialization.impl.compact.integration;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompactUrgentResponseTest extends HazelcastTestSupport {

    private static final ILogger logger = Logger.getLogger(ClientCompactUrgentResponseTest.class);

    protected TestHazelcastFactory factory = new TestHazelcastFactory();

    // Timeout for the tests in this class, you can lower this value when debugging locally
    private static final int TEST_TIMEOUT = 10_000;

    private HazelcastInstance member;

    static class SimpleRecord {
        String name;

        SimpleRecord(String name) {
            this.name = name;
        }

    }

    @Before
    public void setUp() throws Exception {
        member = factory.newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        if (member != null) {
            member.shutdown();
            member = null;
        }
    }

    // ISSUE 1 - sync handler - response always processed on IO thread
    // If you remove the test timeout you should see
    // com.hazelcast.spi.exception.TargetDisconnectedException: Heartbeat timed out to connection ClientConnection\
    // after 10 s and the test passes in ~12 secs
    @Test(timeout = TEST_TIMEOUT)
    public void getCompactValueUsingSyncResponseHandler() {
        HazelcastInstance client1 = factory.newHazelcastClient(new ClientConfig());
        IMap<Integer, SimpleRecord> myMap1 = client1.getMap("my-map");
        // Set value using client 1 - using zero compact serialization
        SimpleRecord record1 = new SimpleRecord("test");
        myMap1.put(1, record1);
        client1.shutdown();

        logger.info("value set to " + record1);

        ClientConfig config2 = new ClientConfig();
        // Due to this property the SyncResponseHandler in ClientResponseHandlerSupplier is used
        config2.setProperty("hazelcast.client.response.thread.count", "0");
        config2.setProperty("hazelcast.client.heartbeat.timeout", "5000");
        config2.setProperty("hazelcast.client.heartbeat.interval", "10000");
        HazelcastInstance client2 = factory.newHazelcastClient(config2);
        try {
            IMap<Integer, SimpleRecord> myMap2 = client2.getMap("my-map");
            SimpleRecord record2 = myMap2.getAsync(1).toCompletableFuture().join();
            assertThat(record2.name).isEqualTo(record1.name);
        } finally {
            client2.shutdown();
        }
    }

    // ISSUE 2 - dynamic response handler - due to the wait it uses IO thread, instead of response thread
    // If you remove the test timeout you should see
    // com.hazelcast.spi.exception.TargetDisconnectedException: Heartbeat timed out to connection ClientConnection\
    // after 10 s and the test passes in ~22 secs
    @Test(timeout = TEST_TIMEOUT)
    public void getCompactValueUsingDynamicResponseHandler() throws Exception {
        HazelcastInstance client1 = factory.newHazelcastClient(new ClientConfig());
        IMap<Integer, SimpleRecord> myMap1 = client1.getMap("my-map");
        // Set value using client 1 - using zero compact serialization
        SimpleRecord record1 = new SimpleRecord("test");
        myMap1.put(1, record1);
        client1.shutdown();

        logger.info("Wrote entry " + record1);

        ClientConfig config2 = new ClientConfig();
        // Use lower timeout and heartbeat for faster test
        config2.setProperty("hazelcast.client.heartbeat.timeout", "5000");
        config2.setProperty("hazelcast.client.heartbeat.interval", "10000");
        HazelcastInstance client2 = factory.newHazelcastClient(config2);
        logger.info("Wait start");
        Thread.sleep(6000);
        logger.info("Wait finished");
        try {
            IMap<Integer, SimpleRecord> myMap2 = client2.getMap("my-map");
            SimpleRecord record2 = myMap2.getAsync(1).toCompletableFuture().join();
            assertThat(record2.name).isEqualTo(record1.name);
        } finally {
            client2.shutdown();
        }
    }

    // ISSUE 3 - async response handler - get data and fetch schema responses on the same thread
    // Removing test timeout shows that this blocks indefinitely
    @Test(timeout = TEST_TIMEOUT)
    public void getCompactValueUsingAsyncResponseHandler() throws Exception {
        HazelcastInstance client1 = factory.newHazelcastClient(new ClientConfig());
        IMap<Integer, SimpleRecord> myMap1 = client1.getMap("my-map");
        // Set value using client 1 - using zero compact serialization
        SimpleRecord record1 = new SimpleRecord("test");
        myMap1.put(1, record1);
        client1.shutdown();

        logger.info("Wrote entry " + record1);

        ClientConfig config2 = new ClientConfig();
        // use only single response thread - it ensures that we hit the issue with same response
        // thread processing the two responses
        config2.setProperty("hazelcast.client.response.thread.count", "1");
        config2.setProperty("hazelcast.client.response.thread.dynamic", "false");
        config2.setProperty("hazelcast.client.heartbeat.timeout", "5000");
        config2.setProperty("hazelcast.client.heartbeat.interval", "10000");
        HazelcastInstance client2 = factory.newHazelcastClient(config2);
        try {
            IMap<Integer, SimpleRecord> myMap2 = client2.getMap("my-map");
            SimpleRecord record2 = myMap2.getAsync(1).toCompletableFuture().join();
            assertThat(record2.name).isEqualTo(record1.name);
        } finally {
            client2.shutdown();
        }
    }

}
