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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImpl_TwoPhaseTest extends HazelcastTestSupport {

    private OperationServiceImpl operationService;
    private ILogger logger;
    private TransactionManagerServiceImpl txManagerService;
    private NodeEngine nodeEngine;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        operationService = getOperationService(hz);
        logger = mock(ILogger.class);

        txManagerService = mock(TransactionManagerServiceImpl.class);
        txManagerService.commitCount = MwCounter.newMwCounter();
        txManagerService.startCount = MwCounter.newMwCounter();
        txManagerService.rollbackCount = MwCounter.newMwCounter();

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(logger);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
    }

    // =================== begin ==================================================

    @Test
    public void begin_whenBeginThrowsException() throws Exception {
        RuntimeException expectedException = new RuntimeException("example exception");
        when(txManagerService.pickBackupLogAddresses(anyInt())).thenThrow(expectedException);

        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, null);
        try {
            tx.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }

        // other independent transaction in same thread
        // should behave identically
        tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        try {
            tx.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }
    }

    // =================== requiresPrepare =======================================

    @Test
    public void requiresPrepare_whenEmpty() throws Exception {
        assertRequiresPrepare(0, false);
    }

    @Test
    public void requiresPrepare_whenLogRecord() throws Exception {
        assertRequiresPrepare(1, false);
    }

    @Test
    public void requiresPrepare_whenMultipleLogRecords() throws Exception {
        assertRequiresPrepare(2, true);
    }

    public void assertRequiresPrepare(int recordCount, boolean expected) throws Exception {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        for (int k = 0; k < recordCount; k++) {
            tx.add(new MockTransactionLogRecord());
        }

        boolean result = tx.requiresPrepare();

        assertEquals(expected, result);
    }

    // =================== prepare ==================================================

    @Test(expected = TransactionException.class)
    public void prepare_whenThrowsExceptionDuringPrepare() throws Exception {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord().failPrepare());

        tx.prepare();
    }

    // =================== commit ==================================================

    @Test(expected = IllegalStateException.class)
    public void commit_whenNotActive() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        tx.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void commit_whenNotPreparedAndMoreThanOneTransactionLogRecord() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord());
        tx.add(new MockTransactionLogRecord());

        tx.commit();
    }

    // there is an optimization for single item transactions so they can commit without preparing
    @Test
    public void commit_whenOneTransactionLogRecord_thenCommit() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord());

        tx.commit();

        assertEquals(COMMITTED, tx.getState());
    }

    @Test
    public void commit_whenThrowsExceptionDuringCommit() throws Exception {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord().failCommit());
        tx.prepare();

        try {
            tx.commit();
            fail();
        } catch (TransactionException expected) {
        }

        assertEquals(COMMIT_FAILED, tx.getState());
    }

    // =================== commit ==================================================

    @Test
    public void rollback() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();

        tx.rollback();

        assertEquals(ROLLED_BACK, tx.getState());
    }

    @Test(expected = IllegalStateException.class)
    public void rollback_whenAlreadyRolledBack() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        tx.rollback();
    }

    @Test
    public void rollback_whenFailureDuringRollback() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord().failRollback());

        tx.rollback();
    }

    @Test
    public void rollback_whenRollingBackCommitFailedTransaction() {
        TransactionOptions options = new TransactionOptions().setTransactionType(TWO_PHASE).setDurability(0);
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
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


    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(
                MockTransactionLogRecord.MockTransactionLogRecordSerializerHook.F_ID,
                new MockTransactionLogRecord.MockTransactionLogRecordSerializerHook().createFactory());
        return config;
    }
}
