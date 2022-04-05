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

package com.hazelcast.internal.iteration;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            Comparable<?> from,
            Comparable<?> to,
            Data lastEntryKeyData
    ) {
        this.flags = flags;
        this.from = from;
        this.to = to;
        this.lastEntryKeyData = lastEntryKeyData;
    }

    public static IndexIterationPointer create(
            Comparable<?> from,
            boolean fromInclusive,
            Comparable<?> to,
            boolean toInclusive,
            boolean descending,
            Data lastEntryKey
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

    public static IndexIterationPointer[] createFromIndexFilter(
            IndexFilter indexFilter,
            boolean descending,
            ExpressionEvalContext evalContext
    ) {
        ArrayList<IndexIterationPointer> result = new ArrayList<>();
        createFromIndexFilterInt(indexFilter, descending, evalContext, result);
        return result.toArray(new IndexIterationPointer[0]);
    }

    private static void createFromIndexFilterInt(
            IndexFilter indexFilter,
            boolean descending,
            ExpressionEvalContext evalContext,
            List<IndexIterationPointer> result
    ) {
        if (indexFilter == null) {
            result.add(create(null, true, null, true, descending, null));
        }
        if (indexFilter instanceof IndexRangeFilter) {
            IndexRangeFilter rangeFilter = (IndexRangeFilter) indexFilter;

            Comparable<?> from = null;
            if (rangeFilter.getFrom() != null) {
                Comparable<?> fromValue = rangeFilter.getFrom().getValue(evalContext);
                // If the index filter has expression like a > NULL, we need to
                // stop creating index iteration pointer because comparison with NULL
                // produces UNKNOWN result.
                if (fromValue == null) {
                    return;
                }
                from = fromValue;
            }

            Comparable<?> to = null;
            if (rangeFilter.getTo() != null) {
                Comparable<?> toValue = rangeFilter.getTo().getValue(evalContext);
                // Same comment above for expressions like a < NULL.
                if (toValue == null) {
                    return;
                }
                to = toValue;
            }

            result.add(create(from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), descending, null));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            result.add(create(value, true, value, true, descending, null));
        } else if (indexFilter instanceof IndexCompositeFilter) {
            IndexCompositeFilter inFilter = (IndexCompositeFilter) indexFilter;
            for (IndexFilter filter : inFilter.getFilters()) {
                createFromIndexFilterInt(filter, descending, evalContext, result);
            }
        }
    }

    public Comparable<?> getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return (flags & FLAG_FROM_INCLUSIVE) != 0;
    }

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
}
