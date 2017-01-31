/*
* Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.hazelcast.quorum.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.quorum.BaseQuorumListenerTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueQuorumListenerTest extends BaseQuorumListenerTest {

    @Test
    public void testQuorumFailureEventFiredWhenNodeCountBelowThreshold() throws Exception {
        final CountDownLatch quorumNotPresent = new CountDownLatch(1);
        final String queueName = randomString();
        final Config config = addQuorum(new Config(), queueName, quorumListener(null, quorumNotPresent));
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<Object> q = instance.getQueue(queueName);
        try {
            q.offer(randomString());
        } catch (Exception expected) {
            expected.printStackTrace();
        }
        assertOpenEventually(quorumNotPresent, 15);
    }

    @Override
    protected void addQuorumConfig(Config config, String distributedObjectName, String quorumName) {
        config.getQueueConfig(distributedObjectName).setQuorumName(quorumName);
    }
}
