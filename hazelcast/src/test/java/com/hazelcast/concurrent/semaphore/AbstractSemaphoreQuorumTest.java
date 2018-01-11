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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public class AbstractSemaphoreQuorumTest {

    protected static final String SEMAPHORE = "quorum" + randomString();
    protected static PartitionedCluster cluster;

    protected static SemaphoreConfig newSemaphoreConfig(QuorumType quorumType, String quorumName) {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
        semaphoreConfig.setName(SEMAPHORE + quorumType.name());
        semaphoreConfig.setQuorumName(quorumName);
        return semaphoreConfig;
    }

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
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
            SemaphoreConfig semaphoreConfig = newSemaphoreConfig(quorumType, quorumName);
            config.addQuorumConfig(quorumConfig);
            config.addSemaphoreConfig(semaphoreConfig);
            quorumNames[i++] = quorumName;
        }

        cluster.createFiveMemberCluster(config);
        for (QuorumType quorumType : types) {
            ISemaphore semaphore = cluster.instance[0].getSemaphore(SEMAPHORE + quorumType.name());
            semaphore.init(100);
        }
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected ISemaphore semaphore(int index, QuorumType quorumType) {
        return cluster.instance[index].getSemaphore(SEMAPHORE + quorumType.name());
    }

}
