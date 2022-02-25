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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.operationservice.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

public class PartitionWideEntryWithPredicateOperation extends PartitionWideEntryOperation {

    private Predicate predicate;

    public PartitionWideEntryWithPredicateOperation() {
    }

    public PartitionWideEntryWithPredicateOperation(String name, EntryProcessor entryProcessor, Predicate predicate) {
        super(name, entryProcessor);
        this.predicate = predicate;
    }

    @Override
    protected Predicate getPredicate() {
        return predicate;
    }

    @Override
    @SuppressFBWarnings(
            value = {"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"},
            justification = "backupProcessor can indeed be null so check is not redundant")
    public Operation getBackupOperation() {
        EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
        if (backupProcessor == null) {
            return null;
        }
        if (keysFromIndex != null) {
            // if we used index we leverage it for the backup too
            return new MultipleEntryBackupOperation(name, keysFromIndex, backupProcessor);
        } else {
            // if no index used we will do a full partition-scan on backup too
            return new PartitionWideEntryWithPredicateBackupOperation(name, backupProcessor, getPredicate());
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", predicate=").append(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY;
    }
}
