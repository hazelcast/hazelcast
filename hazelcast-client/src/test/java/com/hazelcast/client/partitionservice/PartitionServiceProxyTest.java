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

package com.hazelcast.client.partitionservice;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionServiceProxyTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddMigrationListener() throws Exception {
        PartitionService p = client.getPartitionService();
        p.addMigrationListener(new DumMigrationListener());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveMigrationListener() throws Exception {
        PartitionService p = client.getPartitionService();
        p.removeMigrationListener("");
    }

    @Test
    public void testRandomPartitionKeyNotNull() {
        PartitionService p = client.getPartitionService();
        String key = p.randomPartitionKey();
        assertNotNull(key);
    }

    @Test
    public void testGetPartition() {
        String key = "Key";

        PartitionService clientPartitionService = client.getPartitionService();
        Partition clientPartition = clientPartitionService.getPartition(key);

        PartitionService serverPartitionService = server.getPartitionService();
        Partition serverPartition = serverPartitionService.getPartition(key);

        assertEquals(clientPartition.getPartitionId(), serverPartition.getPartitionId());
    }

    @Test
    public void testGetPartitions() {
        String key = "Key";

        PartitionService clientPartitionService = client.getPartitionService();
        Set<Partition> clientPartitions = clientPartitionService.getPartitions();

        PartitionService serverPartitionService = server.getPartitionService();
        Set<Partition> serverPartitions = serverPartitionService.getPartitions();

        assertEquals(clientPartitions.size(), serverPartitions.size());
    }

    class DumMigrationListener implements MigrationListener {
        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
        }
    }
}
