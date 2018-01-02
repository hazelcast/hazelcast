/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_BACKUP_OPERATION;
import static java.lang.Boolean.TRUE;

public class CountDownLatchBackupOperation extends AbstractCountDownLatchOperation
        implements BackupOperation, MutatingOperation {

    private int count;

    public CountDownLatchBackupOperation() {
    }

    public CountDownLatchBackupOperation(String name, int count) {
        super(name);
        this.count = count;
    }

    @Override
    public void run() throws Exception {
        CountDownLatchService service = getService();
        service.setCountDirect(name, count);
    }

    @Override
    public Object getResponse() {
        return TRUE;
    }

    @Override
    public int getId() {
        return COUNT_DOWN_LATCH_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(count);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        count = in.readInt();
    }
}
