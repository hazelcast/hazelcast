/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.flakeidgen;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.FlakeIdGeneratorConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.concurrent.flakeidgen.FlakeIdConcurrencyTestUtil;
import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.FlakeIdGenerator.IdBatch;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(new Supplier<Long>() {
            @Override
            public Long get() {
                return generator.newId();
            }
        });
    }

    @Test
    public void configTest() throws Exception {
        int myBatchSize = 3;
        before(new ClientConfig().addFlakeIdGeneratorConfig(new FlakeIdGeneratorConfig("gen")
                .setPrefetchCount(myBatchSize)
                .setPrefetchValidity(3000)));
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");

        assertTrue("This test assumes default validity be larger than 3000 by a good margin",
                FlakeIdGeneratorConfig.DEFAULT_PREFETCH_VALIDITY > 5000);
        // this should take a batch of 3 IDs from the member and store it in the auto-batcher
        long id1 = generator.newId();
        // this should take another batch, independent from the stored one
        IdBatch batch = generator.newIdBatch(5);
        // the batch start should be out of range for the auto-batch created for id1
        assertTrue(batch.base() > id1 + myBatchSize * batch.increment());
        // this should take second ID from auto-created batch. It should be next to id1
        long id2 = generator.newId();
        assertEquals(id1 + batch.increment(), id2);

        Thread.sleep(3000);
        long id3 = generator.newId();
        assertTrue(batch.base() + batch.increment() * batch.batchSize() < id3);
    }
}
