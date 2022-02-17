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
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPNCounterTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = ConsistencyLostException.class)
    public void testClusterRestart() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        PNCounter pnCounter = client.getPNCounter("test");

        pnCounter.incrementAndGet();

        instance.shutdown();
        hazelcastFactory.newHazelcastInstance();

        pnCounter.incrementAndGet();
    }

}
