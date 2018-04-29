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

package com.hazelcast.concurrent.atomiclong.operations;

import com.hazelcast.concurrent.atomiclong.AtomicLongContainer;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.concurrent.atomiclong.AtomicLongDataSerializerHook.MERGE_BACKUP;

/**
 * Creates backups for merged atomic longs after split-brain healing with a {@link SplitBrainMergePolicy}.
 */
public class MergeBackupOperation extends AbstractAtomicLongOperation implements BackupOperation {

    private Long newValue;

    public MergeBackupOperation() {
    }

    public MergeBackupOperation(String name, Long newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        if (newValue == null) {
            AtomicLongService service = getService();
            service.destroyDistributedObject(name);
        } else {
            AtomicLongContainer container = getLongContainer();
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
        out.writeObject(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readObject();
    }
}
