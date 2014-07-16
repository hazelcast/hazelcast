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

package com.hazelcast.client.partitionservice;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionServiceProxyTest {

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    private PartitionService clientPartitionService;
    private PartitionService serverPartitionService;

    @BeforeClass
    public static void beforeClass() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(null);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        clientPartitionService = client.getPartitionService();
        serverPartitionService = server.getPartitionService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddMigrationListener() throws Exception {
        clientPartitionService.addMigrationListener(new DumMigrationListener());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveMigrationListener() throws Exception {
        clientPartitionService.removeMigrationListener("");
    }

    @Test
    public void testRandomPartitionKeyNotNull() {
        String key = clientPartitionService.randomPartitionKey();
        assertNotNull(key);
    }

    @Test
    public void testGetPartition() {
        String key = "Key";

        Partition clientPartition = clientPartitionService.getPartition(key);
        Partition serverPartition = serverPartitionService.getPartition(key);

        assertEquals(clientPartition.getPartitionId(), serverPartition.getPartitionId());
    }

    @Test
    public void testGetPartitions() {
        Set<Partition> clientPartitions = clientPartitionService.getPartitions();
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
