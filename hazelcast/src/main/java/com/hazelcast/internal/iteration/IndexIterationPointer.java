/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.iteration;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

public class IndexIterationPointer implements IdentifiedDataSerializable {

    private static final byte FLAG_DESCENDING = 1;
    private static final byte FLAG_FROM_INCLUSIVE = 1 << 1;
    private static final byte FLAG_TO_INCLUSIVE = 1 << 2;
    private static final byte FLAG_POINT_LOOKUP = 1 << 3;

    private byte flags;
    private Comparable<?> from;
    private Comparable<?> to;
    private Data lastEntryKeyData;

    public IndexIterationPointer() {
    }

    private IndexIterationPointer(
            byte flags,
            @Nullable Comparable<?> from,
            @Nullable Comparable<?> to,
            @Nullable Data lastEntryKeyData
    ) {
        assert from == null || to == null || ((Comparable) from).compareTo(to) <= 0 : "from must be <= than to";
        assert (from == NULL && to == NULL) || (from != NULL && to != NULL)
                : "IS NULL pointer must point lookup without range or unspecified end: " + from + " ... " + to;
        this.flags = flags;
        this.from = from;
        this.to = to;
        this.lastEntryKeyData = lastEntryKeyData;
    }

    public static IndexIterationPointer create(
            @Nullable Comparable<?> from,
            boolean fromInclusive,
            @Nullable Comparable<?> to,
            boolean toInclusive,
            boolean descending,
            @Nullable Data lastEntryKey
    ) {
        return new IndexIterationPointer(
                (byte) ((descending ? FLAG_DESCENDING : 0)
                        | (fromInclusive ? FLAG_FROM_INCLUSIVE : 0)
                        | (toInclusive ? FLAG_TO_INCLUSIVE : 0)
                        | (from == to ? FLAG_POINT_LOOKUP : 0)),
                from,
                to,
                lastEntryKey
        );
    }

    @Nullable
    public Comparable<?> getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return (flags & FLAG_FROM_INCLUSIVE) != 0;
    }

    @Nullable
    public Comparable<?> getTo() {
        return to;
    }

    public boolean isToInclusive() {
        return (flags & FLAG_TO_INCLUSIVE) != 0;
    }

    public boolean isDescending() {
        return (flags & FLAG_DESCENDING) != 0;
    }

    public Data getLastEntryKeyData() {
        return lastEntryKeyData;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(flags);
        out.writeObject(from);
        if ((flags & FLAG_POINT_LOOKUP) == 0) {
            out.writeObject(to);
        }
        IOUtil.writeData(out, lastEntryKeyData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        flags = in.readByte();
        from = in.readObject();
        if ((flags & FLAG_POINT_LOOKUP) == 0) {
            to = in.readObject();
        } else {
            to = from;
        }
        lastEntryKeyData = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.INDEX_ITERATION_POINTER;
    }

    @Override
    public String toString() {
        return "IndexIterationPointer{"
                + (isFromInclusive() ? "[" : "(") + from + ", " + to + (isToInclusive() ? "]" : ")")
                + (isDescending() ? " DESC" : " ASC")
                + ", lastEntryKeyData=" + lastEntryKeyData
                + "}";
    }
}
