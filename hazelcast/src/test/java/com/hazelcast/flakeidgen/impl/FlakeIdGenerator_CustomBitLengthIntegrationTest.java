/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class FlakeIdGenerator_CustomBitLengthIntegrationTest extends HazelcastTestSupport {
    private static final int BITS_TIMESTAMP = 5;
    private static final int BITS_NODE_ID = 4;
    private static final int BITS_SEQUENCE = 4;
    private static final long PREFETCH_VALIDITY_MS = 10;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void customBitLengthTest() throws Exception {
        Config cfg = new Config();
        cfg.getFlakeIdGeneratorConfig("gen")
                .setBitsNodeId(BITS_NODE_ID)
                .setBitsSequence(BITS_SEQUENCE)
                .setPrefetchValidityMillis(PREFETCH_VALIDITY_MS);
        long maxId = (1L << (BITS_TIMESTAMP + BITS_NODE_ID + BITS_SEQUENCE)) - 1;
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");
        Set<Long> ids = FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(new Supplier<Long>() {
            @Override
            public Long get() {
                return generator.newId() & maxId;
            }
        });
        for (Long id : ids) {
            assertTrue(id + " >= 0", id >= 0);
            assertTrue(id + " <= " + maxId, id <= maxId);
        }
    }
}
