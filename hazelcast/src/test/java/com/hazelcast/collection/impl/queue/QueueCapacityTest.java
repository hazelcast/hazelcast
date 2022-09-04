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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * QueueCapacityTest
 */
public class  QueueCapacityTest {
    private static final String CAPACITY_QUEUE = "TEST_QUEUE";
    private static final int QUEUE_CAPACITY = 2;

    private static HazelcastInstance instance;

    @Before
    public void setup() {
        Config memberConfig = new Config();
        memberConfig.addQueueConfig(new QueueConfig().setName(CAPACITY_QUEUE).setMaxSize(QUEUE_CAPACITY));
        instance = Hazelcast.newHazelcastInstance(memberConfig);
    }

    @Test
    public void testQueueLimitEffective() {
        final IQueue<String> queue = instance.getQueue(CAPACITY_QUEUE);
        final List<String> collection = Collections.nCopies(20, "Hello");
        final boolean added = queue.addAll(collection);
        Assert.assertFalse(added);
        List<String> drainTo = new ArrayList<>();
        queue.drainTo(drainTo);
        Assert.assertEquals(0, drainTo.size());
    }
}
