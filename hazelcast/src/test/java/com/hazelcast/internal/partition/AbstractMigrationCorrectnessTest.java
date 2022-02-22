/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractMigrationCorrectnessTest extends PartitionCorrectnessTestSupport {

    @Parameterized.Parameter(2)
    public boolean fragmentedMigrationEnabled;

    @Test
    public void testPartitionData_whenNodesStartedSequentially() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        for (int i = 1; i <= nodeCount; i++) {
            startNodes(config, 1);
            assertSizeAndDataEventually();
        }
    }

    @Test
    public void testPartitionData_whenNodesStartedParallel() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        startNodes(config, nodeCount);
        assertSizeAndDataEventually();
    }

    @Test
    public void testPartitionData_whenBackupNodesTerminated() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount);
        warmUpPartitions(factory.getAllHazelcastInstances());

        fillData(hz);
        assertSizeAndDataEventually();

        terminateNodes(backupCount);
        assertSizeAndDataEventually();
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenBackupNodesStartedTerminated() throws InterruptedException {
        testPartitionData_whenBackupNodesStartedTerminated(false);
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenBackupNodesStartedTerminated_withSafetyCheckAfterTerminate() throws InterruptedException {
        testPartitionData_whenBackupNodesStartedTerminated(true);
    }

    public void testPartitionData_whenBackupNodesStartedTerminated(boolean checkAfterTerminate)
            throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        int size = 1;
        while (size < (nodeCount + 1)) {
            startNodes(config, backupCount + 1);
            size += (backupCount + 1);

            assertSizeAndDataEventually();

            terminateNodes(backupCount);
            size -= backupCount;

            if (checkAfterTerminate) {
                assertSizeAndDataEventually();
            }
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenBackupNodesStartedTerminated_withRestart() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        Collection<Address> addresses = Collections.emptySet();

        int size = 1;
        while (size < (nodeCount + 1)) {
            int startCount = (backupCount + 1) - addresses.size();
            startNodes(config, addresses);
            startNodes(config, startCount);
            size += (backupCount + 1);

            assertSizeAndDataEventually();

            addresses = terminateNodes(backupCount);
            size -= backupCount;
        }
    }

    @Override
    Config getConfig(boolean withService, boolean antiEntropyEnabled) {
        Config config = super.getConfig(withService, antiEntropyEnabled);
        config.setProperty(ClusterProperty.PARTITION_FRAGMENTED_MIGRATION_ENABLED.getName(), String.valueOf(fragmentedMigrationEnabled));
        return config;
    }
}
