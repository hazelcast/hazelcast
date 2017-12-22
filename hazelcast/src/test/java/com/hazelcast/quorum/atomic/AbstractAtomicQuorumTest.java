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

package com.hazelcast.quorum.atomic;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;

public abstract class AbstractAtomicQuorumTest {

    protected static final String REFERENCE_NAME = "reference" + "quorum" + randomString();
    protected static final String LONG_NAME = "long" + "quorum" + randomString();

    protected static PartitionedCluster cluster;

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
    }

    protected static AtomicReferenceConfig newAtomicReferenceConfig(QuorumType quorumType, String quorumName) {
        AtomicReferenceConfig config = new AtomicReferenceConfig(REFERENCE_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static AtomicLongConfig newAtomicLongConfig(QuorumType quorumType, String quorumName) {
        AtomicLongConfig config = new AtomicLongConfig(LONG_NAME + quorumType.name());
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
            config.addQuorumConfig(quorumConfig);
            config.addAtomicReferenceConfig(newAtomicReferenceConfig(quorumType, quorumName));
            config.addAtomicLongConfig(newAtomicLongConfig(quorumType, quorumName));
            quorumNames[i++] = quorumName;
        }

        cluster.createFiveMemberCluster(config);
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected IAtomicReference aref(int index, QuorumType quorumType) {
        return cluster.instance[index].getAtomicReference(REFERENCE_NAME + quorumType.name());
    }

    protected IAtomicLong along(int index, QuorumType quorumType) {
        return cluster.instance[index].getAtomicLong(LONG_NAME + quorumType.name());
    }

    static class Objekt implements Serializable {
        static Objekt object() {
            return new Objekt();
        }
    }

    static IFunction function() {
        return new IFunction() {
            @Override
            public Object apply(Object input) {
                return input;
            }
        };
    }


}
