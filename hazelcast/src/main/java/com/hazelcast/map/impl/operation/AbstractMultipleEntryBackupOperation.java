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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides common backup operation functionality for {@link com.hazelcast.map.EntryProcessor}
 * that can run on multiple entries.
 */
abstract class AbstractMultipleEntryBackupOperation extends MapOperation {

    protected MapEntries responses;
    protected EntryBackupProcessor backupProcessor;
    protected List<WanEventHolder> wanEventList = Collections.emptyList();

    public AbstractMultipleEntryBackupOperation() {
    }

    public AbstractMultipleEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    protected Predicate getPredicate() {
        return null;
    }

    protected void setWanEventList(List<WanEventHolder> wanEventList) {
        assert wanEventList != null;

        this.wanEventList = wanEventList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(wanEventList.size());
        for (WanEventHolder wanEventHolder : wanEventList) {
            out.writeData(wanEventHolder.getKey());
            out.writeData(wanEventHolder.getValue());
            out.writeInt(wanEventHolder.getEventType().getType());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > 0) {
            wanEventList = new ArrayList<WanEventHolder>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                Data value = in.readData();
                EntryEventType entryEventType = EntryEventType.getByType(in.readInt());
                wanEventList.add(new WanEventHolder(key, value, entryEventType));
            }
        }
    }
}
