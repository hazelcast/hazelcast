/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicHazelcastInstanceAwareTest extends HazelcastTestSupport {
    private CompletableFuture<HazelcastInstance> hazelcastInstanceFuture;

    @Before
    public void setUp() {
        hazelcastInstanceFuture = new CompletableFuture<>();
    }

    @Test
    public void testInjected() {
        createHazelcastInstance().getTopic(randomName()).addMessageListener(new MyMessageListener<>());

        assertTrueEventually(() -> assertNotNull(hazelcastInstanceFuture.get()));
    }

    private class MyMessageListener<V> implements MessageListener<V>, HazelcastInstanceAware {
        @Override
        public void onMessage(Message<V> message) {
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance != null) {
                hazelcastInstanceFuture.complete(hazelcastInstance);
            }
        }
    }
}
