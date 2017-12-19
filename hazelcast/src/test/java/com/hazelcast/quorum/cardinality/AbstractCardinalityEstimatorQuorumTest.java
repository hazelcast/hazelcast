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

package com.hazelcast.quorum.cardinality;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public abstract class AbstractCardinalityEstimatorQuorumTest {

    protected static final String ESTIMATOR_NAME = "quorum" + randomString();

    protected static PartitionedCluster cluster;

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
    }

    protected static CardinalityEstimatorConfig newConfig(QuorumType quorumType, String quorumName) {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(ESTIMATOR_NAME + quorumType.name());
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
            QuorumConfig quorumConfig = newQuorumConfig(quorumType, quorumName);
            CardinalityEstimatorConfig cardinalityEstimatorConfig = newConfig(quorumType, quorumName);
            config.addQuorumConfig(quorumConfig);
            config.addCardinalityEstimatorConfig(cardinalityEstimatorConfig);
            quorumNames[i++] = quorumName;
        }

        cluster.createFiveMemberCluster(config);
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected CardinalityEstimator estimator(int index, QuorumType quorumType) {
        return cluster.instance[index].getCardinalityEstimator(ESTIMATOR_NAME + quorumType.name());
    }

}
