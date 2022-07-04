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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

class DummyBackupOperation extends Operation implements BackupOperation {

    private String backupKey;

    DummyBackupOperation() {
    }

    DummyBackupOperation(String backupKey) {
        this.backupKey = backupKey;
    }

    @Override
    public void run() throws Exception {
        System.out.println("DummyBackupOperation completed");

        if (backupKey == null) {
            return;
        }

        ConcurrentMap<String, Integer> backupCompletedMap = DummyBackupAwareOperation.backupCompletedMap;
        for (; ; ) {
            Integer prev = backupCompletedMap.get(backupKey);
            if (prev == null) {
                Integer found = backupCompletedMap.putIfAbsent(backupKey, 1);
                if (found == null) {
                    return;
                }

                prev = found;
            }

            Integer next = prev + 1;
            if (backupCompletedMap.replace(backupKey, prev, next)) {
                return;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(backupKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupKey = in.readString();
    }
}
