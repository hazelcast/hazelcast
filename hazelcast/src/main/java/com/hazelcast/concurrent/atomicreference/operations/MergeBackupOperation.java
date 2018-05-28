/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceContainer;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook.MERGE_BACKUP;

/**
 * Creates backups for merged atomic references after split-brain healing with a {@link SplitBrainMergePolicy}.
 */
public class MergeBackupOperation extends AbstractAtomicReferenceOperation implements BackupOperation {

    private Data newValue;

    public MergeBackupOperation() {
    }

    public MergeBackupOperation(String name, Data newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        if (newValue == null) {
            AtomicReferenceService service = getService();
            service.destroyDistributedObject(name);
        } else {
            AtomicReferenceContainer container = getReferenceContainer();
            container.set(newValue);
        }
    }

    @Override
    public int getId() {
        return MERGE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readData();
    }
}
