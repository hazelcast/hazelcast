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

package com.hazelcast.client.topic;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.reliable.ReliableTopicDestroyTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientReliableTopicDestroyTest extends ReliableTopicDestroyTest {

    private TestHazelcastFactory factory;
    private HazelcastInstance member;
    private HazelcastInstance client;

    @Override
    protected void createInstances() {
        this.factory = new TestHazelcastFactory();
        this.member = factory.newHazelcastInstance();
        this.client = factory.newHazelcastClient();
    }

    @Override
    protected HazelcastInstance getDriver() {
        return client;
    }

    @Override
    protected HazelcastInstance getMember() {
        return member;
    }

    @After
    public final void terminate() {
        factory.terminateAll();
    }
}
