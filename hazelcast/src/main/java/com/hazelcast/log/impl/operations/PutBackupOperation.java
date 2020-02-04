/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl.operations;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.log.impl.LogContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

import static com.hazelcast.log.impl.LogDataSerializerHook.PUT_BACKUP;

public class PutBackupOperation extends LogOperation implements BackupOperation {

    private Data data;

    public PutBackupOperation() {
    }

    public PutBackupOperation(String name, Data data) {
        super(name);
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        LogContainer container = getContainer();
        container.put(data);
    }

    @Override
    public int getClassId() {
        return PUT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        IOUtil.writeData(out, data);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        data = IOUtil.readData(in);
    }
}
