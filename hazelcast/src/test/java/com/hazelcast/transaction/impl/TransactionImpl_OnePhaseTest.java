package com.hazelcast.transaction.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImpl_OnePhaseTest extends HazelcastTestSupport {

    private InternalOperationService operationService;
    private ILogger logger;
    private TransactionManagerServiceImpl txManagerService;
    private NodeEngine nodeEngine;
    private TransactionOptions options;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        operationService = getOperationService(hz);
        logger = mock(ILogger.class);

        txManagerService = mock(TransactionManagerServiceImpl.class);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(logger);
        options = new TransactionOptions().setTransactionType(ONE_PHASE);
    }

    // ====================== requiresPrepare ===============================

    @Test
    public void requiresPrepare() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();

        assertFalse(tx.requiresPrepare());
    }

    // ====================== prepare ===============================

    @Test(expected = TransactionNotActiveException.class)
    public void prepare_whenNotActive() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.rollback();

        tx.prepare();
    }

    // ====================== commit ===============================

    @Test(expected = IllegalStateException.class)
    public void commit_whenNotActive() {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.rollback();

        tx.commit();
    }

    @Test(expected = TransactionException.class)
    public void commit_ThrowsExceptionDuringCommit() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.add(new MockTransactionLogRecord().failCommit());
        tx.commit();
    }

    // ====================== rollback ===============================

    @Test
    public void rollback_whenEmpty() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.rollback();

        assertEquals(ROLLED_BACK, tx.getState());
    }

    @Test(expected = IllegalStateException.class)
    public void rollback_whenNotActive() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.rollback();

        tx.rollback();
    }

    @Test
    public void rollback_whenRollingBackCommitFailedTransaction() {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        tx.begin();
        tx.add(new MockTransactionLogRecord().failCommit());
        try {
            tx.commit();
            fail();
        } catch (TransactionException expected) {
        }

        tx.rollback();
        assertEquals(ROLLED_BACK, tx.getState());
    }
}
