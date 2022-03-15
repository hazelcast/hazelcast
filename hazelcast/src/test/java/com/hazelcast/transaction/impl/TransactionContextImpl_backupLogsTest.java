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

package com.hazelcast.transaction.impl;

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * A test that makes sure that the backup logs are triggered to be created. Some data-structures like
 * the list/set/queue require it, but others do not.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionContextImpl_backupLogsTest extends HazelcastTestSupport {

    private TransactionManagerServiceImpl localTxManager;
    private TransactionManagerServiceImpl remoteTxManager;
    private UUID ownerUuid;
    private HazelcastInstance localHz;
    private NodeEngineImpl localNodeEngine;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        localHz = cluster[0];
        localNodeEngine = getNodeEngineImpl(localHz);
        localTxManager = getTransactionManagerService(cluster[0]);
        remoteTxManager = getTransactionManagerService(cluster[1]);
        ownerUuid = UuidUtil.newUnsecureUUID();
    }

    private TransactionManagerServiceImpl getTransactionManagerService(HazelcastInstance hz) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        return (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
    }

    @Test
    public void list_backupLogCreationForced() {
        assertBackupLogCreationForced(ListService.SERVICE_NAME);
    }

    @Test
    public void set_backupLogCreationForced() {
        assertBackupLogCreationForced(SetService.SERVICE_NAME);
    }

    @Test
    public void queue_backupLogCreationForced() {
        assertBackupLogCreationForced(QueueService.SERVICE_NAME);
    }

    public void assertBackupLogCreationForced(String serviceName) {
        TransactionOptions options = new TransactionOptions();

        TransactionContextImpl txContext = new TransactionContextImpl(localTxManager, localNodeEngine, options, ownerUuid, false);
        txContext.beginTransaction();

        TransactionalObject result = txContext.getTransactionalObject(serviceName, "foo");

        assertNotNull(result);
        assertNotNull(remoteTxManager.txBackupLogs.get(txContext.getTxnId()));
    }

    @Test
    public void map_thenNotForcesBackupLogCreation() {
        assertBackupLogCreationNotForced(MapService.SERVICE_NAME);
    }

    @Test
    public void multimap_thenNotForceBackupLogCreation() {
        assertBackupLogCreationNotForced(MultiMapService.SERVICE_NAME);
    }

    public void assertBackupLogCreationNotForced(String serviceName) {
        TransactionOptions options = new TransactionOptions();

        TransactionContextImpl txContext = new TransactionContextImpl(localTxManager, localNodeEngine, options, ownerUuid, false);
        txContext.beginTransaction();

        TransactionalObject result = txContext.getTransactionalObject(serviceName, "foo");

        assertNotNull(result);
        assertNull(remoteTxManager.txBackupLogs.get(txContext.getTxnId()));
    }
}
