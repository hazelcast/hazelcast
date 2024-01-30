/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.iterator;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY_ID;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;

/**
 * Container class for a collection of {@link ReplicatedMapEntryViewHolder} along with
 * the cursor id that defines the current page in an iteration.
 * This class is used when iterating ReplicatedMap entry views.
 *
 * @see  com.hazelcast.client.impl.proxy.ClientReplicatedMapProxy#entryViews(int, int)
 */
public class EntryViewsWithCursor implements IdentifiedDataSerializable {

    private List<ReplicatedMapEntryViewHolder> entryViewHolders;
    private UUID cursorId;

    public EntryViewsWithCursor() {
    }

    public EntryViewsWithCursor(@Nonnull List<ReplicatedMapEntryViewHolder> entryViewHolders, @Nonnull UUID cursorId) {
        this.entryViewHolders = entryViewHolders;
        this.cursorId = cursorId;
    }

    @Override
    public int getFactoryId() {
        return REPLICATED_MAP_DS_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.ENTRYVIEWS_WITH_CURSOR;
    }

    @Nonnull
    public List<ReplicatedMapEntryViewHolder> getEntryViewHolders() {
        return entryViewHolders;
    }

    @Nonnull
    public UUID getCursorId() {
        return cursorId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeList(this.entryViewHolders, out);
        UUIDSerializationUtil.writeUUID(out, this.cursorId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.entryViewHolders = readList(in);
        this.cursorId = UUIDSerializationUtil.readUUID(in);
    }
}
