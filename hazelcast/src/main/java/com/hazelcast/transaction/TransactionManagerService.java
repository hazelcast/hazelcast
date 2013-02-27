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

package com.hazelcast.transaction;

import com.hazelcast.core.Transaction;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 2/26/13
 */
public class TransactionManagerService implements ManagedService {

    public final static String SERVICE_NAME = "hz:core:txManagerService";

    private final NodeEngineImpl nodeEngine;

    private final ConcurrentMap<TransactionKey, TransactionLog> txLogs = new ConcurrentHashMap<TransactionKey, TransactionLog>();

    public TransactionManagerService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void onMemberLeft(MemberImpl member) {

    }

    public void prepare(String caller, String txnId, int partitionId, String[] services) {
        txLogs.put(new TransactionKey(txnId, partitionId),
                new TransactionLog(caller, txnId, partitionId, services, Transaction.State.PREPARED));
    }

    public void commit(String caller, String txnId, int partitionId, String[] services) {
        txLogs.put(new TransactionKey(txnId, partitionId),
                new TransactionLog(caller, txnId, partitionId, services, Transaction.State.COMMITTED));
    }

    public void rollback(String caller, String txnId, int partitionId, String[] services) {
        txLogs.put(new TransactionKey(txnId, partitionId),
                new TransactionLog(caller, txnId, partitionId, services, Transaction.State.ROLLED_BACK));
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
    }

    public void shutdown() {
    }

    private static class TransactionKey {
        private final String txnId;
        private final int partitionId;

        private TransactionKey(String txnId, int partitionId) {
            this.txnId = txnId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionLog that = (TransactionLog) o;

            if (partitionId != that.partitionId) return false;
            if (txnId != null ? !txnId.equals(that.txnId) : that.txnId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = txnId != null ? txnId.hashCode() : 0;
            result = 31 * result + partitionId;
            return result;
        }
    }

    private static class TransactionLog {
        private final long logTime = Clock.currentTimeMillis();
        private final String callerUuid;
        private final String txnId;
        private final int partitionId;
        private final String[] services;
        private final TransactionImpl.State state;

        private TransactionLog(String callerUuid, String txnId, int partitionId, String[] services, Transaction.State state) {
            this.callerUuid = callerUuid;
            this.txnId = txnId;
            this.partitionId = partitionId;
            this.services = services;
            this.state = state;
        }

    }
}
