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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.durableexecutor.impl.DurableExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public class PutResultBackupOperation
        extends AbstractDurableExecutorOperation implements BackupOperation {

    private int sequence;

    private Object result;


    public PutResultBackupOperation() {
    }

    public PutResultBackupOperation(String name, int sequence, Object result) {
        super(name);
        this.sequence = sequence;
        this.result = result;
    }

    @Override
    public void run() throws Exception {
        getExecutorContainer().putResult(sequence, result);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(sequence);
        out.writeObject(result);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sequence = in.readInt();
        result = in.readObject();
    }

    @Override
    public int getId() {
        return DurableExecutorDataSerializerHook.PUT_RESULT_BACKUP;
    }
}
