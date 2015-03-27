package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractPartitionLostListenerTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory hazelcastInstanceFactory;

    protected abstract int getNodeCount();

    protected int getMapEntryCount() {
        return 0;
    }

    @Before
    public void createHazelcastInstanceFactory()
            throws IOException {
        hazelcastInstanceFactory = createHazelcastInstanceFactory(getNodeCount());
    }

    @After
    public void terminateAllInstances() {
        hazelcastInstanceFactory.terminateAll();
    }

    final protected void terminateInstances(final List<HazelcastInstance> terminatingInstances) {
        for (HazelcastInstance instance : terminatingInstances) {
            instance.getLifecycleService().terminate();
        }
    }

    final protected List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp(){
        return getCreatedInstancesShuffledAfterWarmedUp(getNodeCount());
    }

    final protected List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp(final int nodeCount) {
        final List<HazelcastInstance> instances = createInstances(nodeCount);
        warmUpPartitions(instances);
        Collections.shuffle(instances);
        return instances;
    }

    final protected List<HazelcastInstance> createInstances(final int nodeCount) {
        final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();
        final Config config = createConfig(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            instances.add(hazelcastInstanceFactory.newHazelcastInstance(config));
        }
        return instances;
    }

    private Config createConfig(final int nodeCount) {
        final Config config = new Config();
        for (int i = 0; i < nodeCount; i++) {
            config.getMapConfig(getIthMapName(i)).setBackupCount(i);
        }

        return config;
    }

    final protected Map<Integer, Integer> getMinReplicaIndicesByPartitionId(final List<HazelcastInstance> instances) {
        final Map<Integer, Integer> survivingPartitions = new HashMap<Integer, Integer>();

        for(HazelcastInstance instance : instances) {
            final Node survivingNode = getNode(instance);
            final Address survivingNodeAddress = survivingNode.getThisAddress();

            for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < getNodeCount(); replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            final Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
                            if (replicaIndexOfOtherInstance != null) {
                                survivingPartitions.put(partition.getPartitionId(),
                                        Math.min(replicaIndex, replicaIndexOfOtherInstance));
                            } else {
                                survivingPartitions.put(partition.getPartitionId(), replicaIndex);
                            }

                            break;
                        }
                    }
                }
            }
        }

        return survivingPartitions;
    }

    final protected void populateMaps(final HazelcastInstance instance) {
        for (int i = 0; i < getNodeCount(); i++) {
            final Map<Integer, Integer> map = instance.getMap(getIthMapName(i));
            for (int j = 0; j < getMapEntryCount(); j++) {
                map.put(j, j);
            }
        }
    }

    final protected String getIthMapName(final int i) {
        return "map-" + i;
    }
}
