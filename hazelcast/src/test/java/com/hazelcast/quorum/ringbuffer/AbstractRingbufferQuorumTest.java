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

package com.hazelcast.quorum.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public abstract class AbstractRingbufferQuorumTest {

    protected static final String RINGBUFFER_NAME = "quorum" + randomString();

    protected static PartitionedCluster cluster;

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
    }

    protected static RingbufferConfig newConfig(QuorumType quorumType, String quorumName) {
        RingbufferConfig ringbufferConfig = new RingbufferConfig(RINGBUFFER_NAME + quorumType.name());
        ringbufferConfig.setQuorumName(quorumName);
        return ringbufferConfig;
    }

    protected static QuorumConfig newQuorumConfig(QuorumType quorumType, String quorumName) {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(quorumName);
        quorumConfig.setType(quorumType);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        return quorumConfig;
    }

    protected static void initCluster(Config config, TestHazelcastInstanceFactory factory, QuorumType... types) {
        cluster = new PartitionedCluster(factory);

        String[] quorumNames = new String[types.length];
        int i = 0;
        for (QuorumType quorumType : types) {
            String quorumName = QUORUM_ID + quorumType.name();
            QuorumConfig quorumConfig = newQuorumConfig(quorumType, quorumName);
            RingbufferConfig ringbufferConfig = newConfig(quorumType, quorumName);
            config.addQuorumConfig(quorumConfig);
            config.addRingBufferConfig(ringbufferConfig);
            quorumNames[i++] = quorumName;
        }

        cluster.createFiveMemberCluster(config);
        for (QuorumType quorumType : types) {
            for (int id = 0; id < 5; id++) {
                Ringbuffer ringbuffer = cluster.instance[0].getRingbuffer(RINGBUFFER_NAME + quorumType.name());
                ringbuffer.add(String.valueOf(id));
            }
        }
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected Ringbuffer ring(int index, QuorumType quorumType) {
        return cluster.instance[index].getRingbuffer(RINGBUFFER_NAME + quorumType.name());
    }

}
