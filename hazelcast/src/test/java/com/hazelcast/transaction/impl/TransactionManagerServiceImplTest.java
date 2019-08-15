/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl.TxBackupLog;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

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
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);

        assertTxLogState(txId, ACTIVE);
    }

    @Test(expected = TransactionException.class)
    public void createBackupLog_whenAlreadyExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);

        txService.createBackupLog(callerUuid, txId);
    }

    // ================= rollbackBackupLog ===================================

    @Test(expected = TransactionException.class)
    public void replicaBackupLog_whenNotExist_thenTransactionException() {
        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, UuidUtil.newUnsecureUUID(), "notexist", 1, 1);
    }

    @Test
    public void replicaBackupLog_whenExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);

        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, callerUuid, txId, 1, 1);

        assertTxLogState(txId, COMMITTING);
    }

    @Test(expected = TransactionException.class)
    public void replicaBackupLog_whenNotActive() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);
        txService.txBackupLogs.get(txId).state = ROLLED_BACK;

        List<TransactionLogRecord> records = new LinkedList<>();
        txService.replicaBackupLog(records, callerUuid, txId, 1, 1);
    }

    private void assertTxLogState(String txId, Transaction.State state) {
        TxBackupLog backupLog = txService.txBackupLogs.get(txId);
        assertNotNull(backupLog);
        assertEquals(state, backupLog.state);
    }

    // ================= rollbackBackupLog ===================================

    @Test
    public void rollbackBackupLog_whenExist() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);

        txService.rollbackBackupLog(txId);

        assertTxLogState(txId, ROLLING_BACK);
    }

    @Test
    public void rollbackBackupLog_whenNotExist_thenIgnored() {
        txService.rollbackBackupLog("notexist");
    }

    // ================= purgeBackupLog ===================================

    @Test
    public void purgeBackupLog_whenExist_thenRemoved() {
        UUID callerUuid = UuidUtil.newUnsecureUUID();
        String txId = "tx1";
        txService.createBackupLog(callerUuid, txId);

        txService.purgeBackupLog(txId);

        assertFalse(txService.txBackupLogs.containsKey(txId));
    }

    @Test
    public void purgeBackupLog_whenNotExist_thenIgnored() {
        txService.purgeBackupLog("notexist");
    }
}
