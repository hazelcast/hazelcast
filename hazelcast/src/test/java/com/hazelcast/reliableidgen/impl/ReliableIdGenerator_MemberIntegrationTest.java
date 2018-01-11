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

package com.hazelcast.reliableidgen.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.reliableidgen.ReliableIdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_9;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReliableIdGenerator_MemberIntegrationTest extends HazelcastTestSupport {

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
        final ReliableIdGenerator generator = instance.getReliableIdGenerator("gen");
        ReliableIdConcurrencyTestUtil.concurrentlyGenerateIds(new Supplier<Long>() {
            @Override
            public Long get() {
                return generator.newId();
            }
        });
    }

    @Test
    public void when_310MemberJoinsWith39Mode_reliableIdGeneratorDoesNotWork() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_9.toString());
        HazelcastInstance instance = factory.newHazelcastInstance();

        ReliableIdGenerator gen = instance.getReliableIdGenerator("gen");
        exception.expect(UnsupportedOperationException.class);
        gen.newId();
    }
}
