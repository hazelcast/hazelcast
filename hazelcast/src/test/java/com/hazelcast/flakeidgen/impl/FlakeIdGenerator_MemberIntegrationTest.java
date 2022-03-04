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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
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

import java.util.Map;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class FlakeIdGenerator_MemberIntegrationTest extends HazelcastTestSupport {

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
    public void smokeTest() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");
        FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(generator::newId);
    }

    @Test

    public void statistics() {
        HazelcastInstance instance = factory.newHazelcastInstance();

        FlakeIdGenerator gen = instance.getFlakeIdGenerator("gen");
        gen.newId();

        FlakeIdGeneratorService service = getNodeEngineImpl(instance).getService(FlakeIdGeneratorService.SERVICE_NAME);
        Map<String, LocalFlakeIdGeneratorStats> stats = service.getStats();
        assertTrue(!stats.isEmpty());
        assertTrue(stats.containsKey("gen"));
        LocalFlakeIdGeneratorStats genStats = stats.get("gen");
        assertEquals(1L, genStats.getBatchCount());
        assertTrue(genStats.getIdCount() > 0);
    }
}
