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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class FlakeIdGenerator_MemberBackpressureTest extends FlakeIdGenerator_AbstractBackpressureTest {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        factory = new TestHazelcastInstanceFactory();
        instance = factory.newHazelcastInstance(new Config().addFlakeIdGeneratorConfig(new FlakeIdGeneratorConfig("gen")
                .setPrefetchCount(BATCH_SIZE)));
    }

    @After
    public void after() {
        factory.shutdownAll();
    }
}
