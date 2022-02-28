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
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.Accessors.getOperationService;
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

    private OperationServiceImpl operationService;
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
        txManagerService.commitCount = MwCounter.newMwCounter();
        txManagerService.startCount = MwCounter.newMwCounter();
        txManagerService.rollbackCount = MwCounter.newMwCounter();

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(logger);
        options = new TransactionOptions().setTransactionType(ONE_PHASE);
    }

    // ====================== requiresPrepare ===============================

    @Test
    public void requiresPrepare() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();

        assertFalse(tx.requiresPrepare());
    }

    // ====================== prepare ===============================

    @Test(expected = TransactionNotActiveException.class)
    public void prepare_whenNotActive() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        tx.prepare();
    }

    // ====================== commit ===============================

    @Test(expected = IllegalStateException.class)
    public void commit_whenNotActive() {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        tx.commit();
    }

    @Test(expected = TransactionException.class)
    public void commit_ThrowsExceptionDuringCommit() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.add(new MockTransactionLogRecord().failCommit());
        tx.commit();
    }

    // ====================== rollback ===============================

    @Test
    public void rollback_whenEmpty() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        assertEquals(ROLLED_BACK, tx.getState());
    }

    @Test(expected = IllegalStateException.class)
    public void rollback_whenNotActive() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        tx.begin();
        tx.rollback();

        tx.rollback();
    }

    @Test
    public void rollback_whenRollingBackCommitFailedTransaction() {
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
