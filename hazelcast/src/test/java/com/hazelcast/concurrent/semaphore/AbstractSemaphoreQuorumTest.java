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
    protected static PartitionedCluster CLUSTER;

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
        CLUSTER = null;
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
        CLUSTER = new PartitionedCluster(factory);

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

        CLUSTER.createFiveMemberCluster(config);
        for (QuorumType quorumType : types) {
            ISemaphore semaphore = CLUSTER.instance[0].getSemaphore(SEMAPHORE + quorumType.name());
            semaphore.init(100);
        }
        CLUSTER.splitFiveMembersThreeAndTwo(quorumNames);
    }

    protected ISemaphore semaphore(int index, QuorumType quorumType) {
        return CLUSTER.instance[index].getSemaphore(SEMAPHORE + quorumType.name());
    }

}
