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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.Predicate;

import java.io.IOException;

/**
 * Provides common backup operation functionality for {@link com.hazelcast.map.EntryProcessor}
 * that can run on multiple entries.
 */
abstract class AbstractMultipleEntryBackupOperation extends MapOperation implements Versioned {

    EntryBackupProcessor backupProcessor;

    public AbstractMultipleEntryBackupOperation() {
    }

    public AbstractMultipleEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // RU_COMPAT_3_9
        if (out.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            out.writeInt(0);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // RU_COMPAT_3_9
        if (in.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            final int size = in.readInt();
            for (int i = 0; i < size; i++) {
                // key
                in.readData();
                // value
                in.readData();
                // event type
                in.readInt();
            }
        }
    }
}
