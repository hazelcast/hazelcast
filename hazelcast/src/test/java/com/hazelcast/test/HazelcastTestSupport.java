/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import org.junit.After;
import org.junit.runner.RunWith;

/**
 * @author mdogan 5/24/13
 */

@RunWith(HazelcastJUnit4ClassRunner.class)
public abstract class HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(nodeCount);
    }

    @After
    public final void shutdownNodeFactory() {
        final TestHazelcastInstanceFactory f = factory;
        if (f != null) {
            factory = null;
            f.shutdownAll();
        }
    }

    public static Node getNode(HazelcastInstance hz) {
        return TestUtil.getNode(hz);
    }

    protected static void warmUpPartitions(HazelcastInstance... instances) throws InterruptedException {
        TestUtil.warmUpPartitions(instances);
    }

    protected static String generateKeyOwnedBy(HazelcastInstance instance) throws InterruptedException {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        int k = (int) (Math.random() * 1000);
        while (!localMember.equals(partitionService.getPartition(String.valueOf(k)).getOwner())) {
            k++;
            Thread.sleep(10);
        }
        return String.valueOf(k);
    }

    protected static String generateKeyNotOwnedBy(HazelcastInstance instance) throws InterruptedException {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        int k = (int) (Math.random() * 1000);
        while (localMember.equals(partitionService.getPartition(String.valueOf(k)).getOwner())) {
            k++;
            Thread.sleep(10);
        }
        return String.valueOf(k);
    }

    public final class DummyUncheckedHazelcastTestException extends RuntimeException{

    }
}
