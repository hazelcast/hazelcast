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
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAResource;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionServiceProxyTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(null);
    }

    @AfterClass
    public static void destroy(){
        client.shutdown();
        server.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addMigrationListener_test() throws Exception {
        PartitionService p = client.getPartitionService();
        p.addMigrationListener(new DumMigrationListener());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeMigrationListener_test() throws Exception {
        PartitionService p = client.getPartitionService();
        p.removeMigrationListener("");
    }

    @Test
    public void randomPartitionKey_notNullTest() {
        PartitionService p = client.getPartitionService();
        String key = p.randomPartitionKey();
        assertNotNull(key);
    }

    @Test
    public void getPartition_test() {
        String key = "Key";

        PartitionService client_ps = client.getPartitionService();
        Partition client_p  = client_ps.getPartition(key);

        PartitionService server_ps = server.getPartitionService();
        Partition server_p = server_ps.getPartition(key);

        assertEquals(client_p.getPartitionId(), server_p.getPartitionId());
    }

    @Test
    public void getPartitions_test() {
        String key = "Key";

        PartitionService client_ps = client.getPartitionService();
        Set<Partition> clientPartitions = client_ps.getPartitions();

        PartitionService server_ps = server.getPartitionService();
        Set<Partition> serverPartitions = server_ps.getPartitions();


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
