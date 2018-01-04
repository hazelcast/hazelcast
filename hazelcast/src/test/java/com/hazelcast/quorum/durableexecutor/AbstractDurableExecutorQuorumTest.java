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

package com.hazelcast.quorum.durableexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Arrays.asList;

public abstract class AbstractDurableExecutorQuorumTest {

    protected static final String EXEC_NAME = "quorum" + randomString();

    protected static PartitionedCluster cluster;

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
    }

    protected static DurableExecutorConfig newExecConfig(QuorumType quorumType, String quorumName, String postfix) {
        DurableExecutorConfig config = new DurableExecutorConfig(EXEC_NAME + quorumType.name() + postfix);
        config.setQuorumName(quorumName);
        return config;
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
            quorumNames[i++] = quorumName;
            for (String postfix : asList("", "shutdown", "shutdownNow")) {
                QuorumConfig quorumConfig = newQuorumConfig(quorumType, quorumName);
                DurableExecutorConfig execConfig = newExecConfig(quorumType, quorumName, postfix);
                config.addQuorumConfig(quorumConfig);
                config.addDurableExecutorConfig(execConfig);
            }
        }

        cluster.createFiveMemberCluster(config);
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected DurableExecutorService exec(int index, QuorumType quorumType) {
        return exec(index, quorumType, "");
    }

    protected DurableExecutorService exec(int index, QuorumType quorumType, String postfix) {
        return cluster.instance[index].getDurableExecutorService(EXEC_NAME + quorumType.name() + postfix);
    }

}
