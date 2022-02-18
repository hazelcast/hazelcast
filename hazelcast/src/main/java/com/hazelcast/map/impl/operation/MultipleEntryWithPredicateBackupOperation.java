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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class MultipleEntryWithPredicateBackupOperation extends MultipleEntryBackupOperation {

    private Predicate predicate;

    public MultipleEntryWithPredicateBackupOperation() {
    }

    public MultipleEntryWithPredicateBackupOperation(String name, Set<Data> keys,
                                                     EntryProcessor backupProcessor, Predicate predicate) {
        super(name, keys, backupProcessor);
        this.predicate = checkNotNull(predicate, "predicate cannot be null");
    }

    @Override
    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        predicate = in.readObject();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MULTIPLE_ENTRY_PREDICATE_BACKUP;
    }
}
