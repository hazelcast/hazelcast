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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl.TxBackupLog;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionManagerServiceImplTest extends HazelcastTestSupport {
    private static final UUID TXN = UUID.randomUUID();

    private TransactionManagerServiceImpl txService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        txService = new TransactionManagerServiceImpl(nodeEngine);
    }

    // ================= createBackupLog ===================================

    @Test
    public void createBackupLog_whenNotCreated() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);

        assertTxLogState(TXN, ACTIVE);
    }

    @Test(expected = TransactionException.class)
    public void createBackupLog_whenAlreadyExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);

        txService.createBackupLog(callerUuid, TXN);
    }

    // ================= rollbackBackupLog ===================================

    @Test(expected = TransactionException.class)
    public void replicaBackupLog_whenNotExist_thenTransactionException() {
        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, UuidUtil.newUnsecureUUID(), TXN, 1, 1);
    }

    @Test
    public void replicaBackupLog_whenExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);

        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, callerUuid, TXN, 1, 1);

        assertTxLogState(TXN, COMMITTING);
    }

    @Test(expected = TransactionException.class)
    public void replicaBackupLog_whenNotActive() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);
        txService.txBackupLogs.get(TXN).state = ROLLED_BACK;

        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, callerUuid, TXN, 1, 1);
    }

    private void assertTxLogState(UUID txId, Transaction.State state) {
        TxBackupLog backupLog = txService.txBackupLogs.get(txId);
        assertNotNull(backupLog);
        assertEquals(state, backupLog.state);
    }

    // ================= rollbackBackupLog ===================================

    @Test
    public void rollbackBackupLog_whenExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);

        txService.rollbackBackupLog(TXN);

        assertTxLogState(TXN, ROLLING_BACK);
    }

    @Test
    public void rollbackBackupLog_whenNotExist_thenIgnored() {
        txService.rollbackBackupLog(TXN);
    }

    // ================= purgeBackupLog ===================================

    @Test
    public void purgeBackupLog_whenExist_thenRemoved() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        txService.createBackupLog(callerUuid, TXN);

        txService.purgeBackupLog(TXN);

        assertFalse(txService.txBackupLogs.containsKey(TXN));
    }

    @Test
    public void purgeBackupLog_whenNotExist_thenIgnored() {
        txService.purgeBackupLog(TXN);
    }
}
