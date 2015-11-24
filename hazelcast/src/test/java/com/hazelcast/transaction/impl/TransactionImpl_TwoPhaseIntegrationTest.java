package com.hazelcast.transaction.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl.TxBackupLog;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImpl_TwoPhaseIntegrationTest extends HazelcastTestSupport {

    private HazelcastInstance[] cluster;
    private TransactionManagerServiceImpl localTxService;
    private TransactionManagerServiceImpl remoteTxService;
    private NodeEngineImpl localNodeEngine;
    private String txOwner;

    @Before
    public void setup() {
        setLoggingLog4j();
        cluster = createHazelcastInstanceFactory(2).newInstances();
        localNodeEngine = getNodeEngineImpl(cluster[0]);
        localTxService = getTransactionManagerService(cluster[0]);
        remoteTxService = getTransactionManagerService(cluster[1]);
        txOwner = UuidUtil.newUnsecureUuidString();
    }

    private void assertPrepared(TransactionImpl tx) {
        assertEquals(PREPARED, tx.getState());
    }

    private void assertCommitted(TransactionImpl tx) {
        assertEquals(COMMITTED, tx.getState());
    }

    private void assertRolledBack(TransactionImpl tx) {
        assertEquals(ROLLED_BACK, tx.getState());
    }

    private void assertCommitFailed(TransactionImpl tx) {
        assertEquals(COMMIT_FAILED, tx.getState());
    }

    private void assertPreparing(TransactionImpl tx) {
        assertEquals(PREPARING, tx.getState());
    }

    private TransactionManagerServiceImpl getTransactionManagerService(HazelcastInstance hz) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        return (TransactionManagerServiceImpl) nodeEngineImpl.getTransactionManagerService();
    }

    // =================== prepare ===========================================

    @Test
    public void prepare_whenSingleItemAndDurabilityOne_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record = new MockTransactionLogRecord();
        tx.add(record);

        tx.prepare();

        assertPrepared(tx);
        assertNoBackupLogOnRemote(tx);
        record.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
    }

    private void assertNoBackupLogOnRemote(TransactionImpl tx) {
        TxBackupLog log = remoteTxService.txBackupLogs.get(tx.getTxnId());
        assertNull(log);
    }

    @Test
    public void prepare_whenMultipleItemsAndDurabilityOne_thenBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);

        tx.prepare();

        assertPrepared(tx);
        TxBackupLog log = remoteTxService.txBackupLogs.get(tx.getTxnId());
        assertNotNull(log);
        assertEquals(COMMITTING, log.state);
        record1.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
        record2.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
    }

    @Test
    public void prepare_whenMultipleItemsAndDurabilityZero_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);

        tx.prepare();

        assertPrepared(tx);
        assertNoBackupLogOnRemote(tx);
        record1.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
        record2.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
    }

    @Test
    public void testPrepareFailed() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record = new MockTransactionLogRecord().failPrepare();
        tx.add(record);

        try {
            tx.prepare();
            fail();
        } catch (TransactionException expected) {
        }

        assertPreparing(tx);
        assertNoBackupLogOnRemote(tx);
        record.assertPrepareCalled().assertCommitNotCalled().assertRollbackNotCalled();
    }

    // =================== commit ===========================================

    @Test
    public void commit_whenSingleItemAndDurabilityOne_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record = new MockTransactionLogRecord();
        tx.add(record);
        tx.prepare();

        tx.commit();

        assertCommitted(tx);
        assertNoBackupLogOnRemote(tx);
        record.assertPrepareCalled().assertCommitCalled().assertRollbackNotCalled();
    }

    @Test
    public void commit_whenMultipleItemsAndDurabilityOne_thenBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        final TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);
        tx.prepare();

        tx.commit();

        assertCommitted(tx);
        // it can take some time because the transaction doesn't sync on purging the backups.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNoBackupLogOnRemote(tx);
            }
        });
        record1.assertPrepareCalled().assertCommitCalled().assertRollbackNotCalled();
        record2.assertPrepareCalled().assertCommitCalled().assertRollbackNotCalled();
    }

    @Test
    public void commit_whenMultipleItemsAndDurabilityZero_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);
        tx.prepare();

        tx.commit();

        assertCommitted(tx);
        assertNoBackupLogOnRemote(tx);
        record1.assertPrepareCalled().assertCommitCalled().assertRollbackNotCalled();
        record2.assertPrepareCalled().assertCommitCalled().assertRollbackNotCalled();
    }

    @Test
    public void commit_whenPrepareSkippedButCommitRunsIntoConflict() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record = new MockTransactionLogRecord().failCommit();
        tx.add(record);

        try {
            tx.commit();
            fail();
        } catch (TransactionException expected) {
        }

        assertCommitFailed(tx);
        assertNoBackupLogOnRemote(tx);
        record.assertPrepareNotCalled().assertCommitCalled().assertRollbackNotCalled();
    }

    // =================== rollback ===========================================

    @Test
    public void rollback_whenSingleItemAndDurabilityOne_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record = new MockTransactionLogRecord();
        tx.add(record);
        tx.prepare();

        tx.rollback();

        assertRolledBack(tx);
        assertNoBackupLogOnRemote(tx);
        record.assertPrepareCalled().assertCommitNotCalled().assertRollbackCalled();
    }

    @Test
    public void rollback_whenMultipleItemsAndDurabilityOne_thenBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(1);
        final TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);
        tx.prepare();

        tx.rollback();

        assertRolledBack(tx);
        // it can take some time because the transaction doesn't sync on purging the backups.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNoBackupLogOnRemote(tx);
            }
        });
        record1.assertPrepareCalled().assertCommitNotCalled().assertRollbackCalled();
        record2.assertPrepareCalled().assertCommitNotCalled().assertRollbackCalled();
    }

    @Test
    public void rollback_whenMultipleItemsAndDurabilityZero_thenNoBackupLog() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(localTxService, localNodeEngine, options, txOwner);
        tx.begin();
        MockTransactionLogRecord record1 = new MockTransactionLogRecord();
        tx.add(record1);
        MockTransactionLogRecord record2 = new MockTransactionLogRecord();
        tx.add(record2);
        tx.prepare();

        tx.rollback();

        assertRolledBack(tx);
        assertNoBackupLogOnRemote(tx);
        record1.assertPrepareCalled().assertCommitNotCalled().assertRollbackCalled();
        record2.assertPrepareCalled().assertCommitNotCalled().assertRollbackCalled();
    }
}
