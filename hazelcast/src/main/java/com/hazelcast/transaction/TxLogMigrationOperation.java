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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @mdogan 3/1/13
 */

public class TxLogMigrationOperation extends AbstractOperation {

    private Collection<TransactionLog> logs;

    public TxLogMigrationOperation() {
    }

    public TxLogMigrationOperation(Collection<TransactionLog> logs) {
        this.logs = logs;
    }

    public void run() throws Exception {
        TransactionManagerServiceImpl txService = getService();
        for (TransactionLog log : logs) {
            txService.addLog(log);
        }
    }

    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = logs != null ? logs.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (TransactionLog log : logs) {
                log.writeData(out);
            }
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        logs = new ArrayList<TransactionLog>(len);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                TransactionLog log = new TransactionLog();
                log.readData(in);
                logs.add(log);
            }
        }
    }
}
