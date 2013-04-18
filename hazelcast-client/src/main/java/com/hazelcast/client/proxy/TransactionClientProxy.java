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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionLog;

public class TransactionClientProxy implements Transaction {
    final ProxyHelper proxyHelper;

    public TransactionClientProxy(HazelcastClient client) {
        proxyHelper = new ProxyHelper(client);
    }

    public void begin() throws IllegalStateException {
        proxyHelper.ensureContextHasConnection(null);
        proxyHelper.doCommand(Command.TRXBEGIN, new String[]{});
    }

    public void commit() throws IllegalStateException {
        Context context = Context.get();
        checkNull(context);
        proxyHelper.doCommand(Command.TRXCOMMIT, new String[]{});
        release(context);
    }

    private void checkNull(Context context) {
        if (context == null) {
            throw new IllegalStateException("Transaction is not active");
        }
    }

    private void release(Context context) {
        if (context.noMoreLocks()) {
            proxyHelper.cp.releaseConnection(context.getConnection());
            Context.remove();
        }
    }

    public int getStatus() {
        return proxyHelper.doCommandAsInt(Command.TRXSTATUS, new String[]{});
    }



    public void addTransactionLog(TransactionLog transactionLog) {

    }

    public TransactionLog getTransactionLog(Object key) {
        return null;
    }

    public String getTxnId() {
        return null;
    }

    public State getState() {
        return null;
    }

    @Override
    public long getTimeoutMillis() {
        return 0;
    }

    public void rollback() throws IllegalStateException {
        Context context = Context.get();
        checkNull(context);
        proxyHelper.doCommand(Command.TRXROLLBACK, new String[]{});
        release(context);
    }

    public void setTransactionTimeout(int seconds) {
    }

    public long getTimeoutSeconds() {
        return 0;
    }
}
