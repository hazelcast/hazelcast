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

package com.hazelcast.client.pncounter;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.crdt.pncounter.AbstractPNCounterNoDataMemberTest;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Client implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPNCounterNoDataMemberTest extends AbstractPNCounterNoDataMemberTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private String counterName = randomMapName("counter-");
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance(new Config().setLiteMember(true));
        client = hazelcastFactory.newHazelcastClient();
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(5);
    }

    @Override
    protected PNCounter getCounter() {
        return client.getPNCounter(counterName);
    }
}
