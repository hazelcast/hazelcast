/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.queue;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public abstract class AbstractQueueQuorumTest {

    protected static final String QUEUE_NAME_PREFIX = "quorum";

    protected static final int QUEUE_DATA_COUNT = 50;
    protected static final String QUEUE_NAME = QUEUE_NAME_PREFIX + randomString();

    protected static PartitionedCluster cluster;
    protected static IQueue<Object> q1;
    protected static IQueue<Object> q2;
    protected static IQueue<Object> q3;
    protected static IQueue<Object> q4;
    protected static IQueue<Object> q5;

    protected static void initializeFiveMemberCluster(QuorumType type, int quorumSize) {
        QuorumConfig quorumConfig = new QuorumConfig()
                .setName(QUORUM_ID)
                .setType(type)
                .setEnabled(true)
                .setSize(quorumSize);
        QueueConfig qConfig = new QueueConfig(QUEUE_NAME_PREFIX + "*")
                .setBackupCount(4)
                .setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster(new TestHazelcastInstanceFactory());
        cluster.createFiveMemberCluster(qConfig, quorumConfig);
        q1 = getQueue(cluster.h1);
        q2 = getQueue(cluster.h2);
        q3 = getQueue(cluster.h3);
        q4 = getQueue(cluster.h4);
        q5 = getQueue(cluster.h5);
    }

    protected static <E> IQueue<E> getQueue(HazelcastInstance instance) {
        return instance.getQueue(QUEUE_NAME);
    }

    protected static void addQueueData(IQueue<Object> q) {
        for (int i = 0; i < QUEUE_DATA_COUNT; i++) {
            q.add("foo" + i);
        }
    }

    @Test
    public void testOperationsSuccessfulWhenQuorumSizeMet() throws Exception {
        q1.put("foo");
        q2.offer("bar");
        q3.add("bar2");

        q3.take();
        q1.remove("bar");
        q2.poll();
        q1.element();
        q2.peek();

        q3.getLocalQueueStats();
    }
}
