package com.hazelcast.transaction.impl;

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
    private String ownerUuid;
    private HazelcastInstance localHz;
    private NodeEngineImpl localNodeEngine;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        localHz = cluster[0];
        localNodeEngine = getNodeEngineImpl(localHz);
        localTxManager = getTransactionManagerService(cluster[0]);
        remoteTxManager = getTransactionManagerService(cluster[1]);
        ownerUuid = UuidUtil.newUnsecureUuidString();
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

        TransactionContextImpl txContext = new TransactionContextImpl(localTxManager, localNodeEngine, options, ownerUuid);
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

        TransactionContextImpl txContext = new TransactionContextImpl(localTxManager, localNodeEngine, options, ownerUuid);
        txContext.beginTransaction();

        TransactionalObject result = txContext.getTransactionalObject(serviceName, "foo");

        assertNotNull(result);
        assertNull(remoteTxManager.txBackupLogs.get(txContext.getTxnId()));
    }
}
