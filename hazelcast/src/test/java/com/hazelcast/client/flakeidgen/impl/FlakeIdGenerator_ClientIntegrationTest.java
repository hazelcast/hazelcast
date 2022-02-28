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

package com.hazelcast.client.flakeidgen.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.flakeidgen.impl.FlakeIdConcurrencyTestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_NODE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGenerator_ClientIntegrationTest {

    private TestHazelcastFactory factory;
    private HazelcastInstance instance;

    public void before(ClientConfig config) {
        factory = new TestHazelcastFactory(2);
        HazelcastInstance[] instances = factory.newInstances();
        instance = factory.newHazelcastClient(config);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void smokeTest() throws Exception {
        before(null);
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");
        FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(generator::newId);
    }

    @Test
    public void configTest() throws Exception {
        int myBatchSize = 3;
        ClientFlakeIdGeneratorConfig clientFlakeIdGeneratorConfig = new ClientFlakeIdGeneratorConfig("gen")
                .setPrefetchCount(myBatchSize)
                .setPrefetchValidityMillis(3000);
        before(new ClientConfig().addFlakeIdGeneratorConfig(clientFlakeIdGeneratorConfig));
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");

        assertTrue("This test assumes default validity be larger than 3000 by a good margin",
                FlakeIdGeneratorConfig.DEFAULT_PREFETCH_VALIDITY_MILLIS >= 5000);
        // this should take a batch of 3 IDs from the member and store it in the auto-batcher
        long id1 = generator.newId();
        // this should take second ID from auto-created batch. It should be exactly next to id1
        long id2 = generator.newId();
        long increment = 1 << DEFAULT_BITS_NODE_ID;
        assertEquals(id1 + increment, id2);

        Thread.sleep(3000);
        // this ID should be from a new batch, because the validity elapsed
        long id3 = generator.newId();
        assertTrue(id1 + increment * myBatchSize < id3);
    }
}
