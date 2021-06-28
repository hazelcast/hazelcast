/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
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

    public IndexIterationPointer() {
    }

    private IndexIterationPointer(
            byte flags,
            Comparable<?> from,
            Comparable<?> to
    ) {
        this.flags = flags;
        this.from = from;
        this.to = to;
    }

    public static IndexIterationPointer create(
            Comparable<?> from,
            boolean fromInclusive,
            Comparable<?> to,
            boolean toInclusive,
            boolean descending
    ) {
        return new IndexIterationPointer(
                (byte) ((descending ? FLAG_DESCENDING : 0)
                        | (fromInclusive ? FLAG_FROM_INCLUSIVE : 0)
                        | (toInclusive ? FLAG_TO_INCLUSIVE : 0)
                        | (from == to ? FLAG_POINT_LOOKUP : 0)),
                from,
                to);
    }

    public static IndexIterationPointer[] createFromIndexFilter(IndexFilter indexFilter, ExpressionEvalContext evalContext) {
        ArrayList<IndexIterationPointer> result = new ArrayList<>();
        createFromIndexFilterInt(indexFilter, evalContext, result);
        return result.toArray(new IndexIterationPointer[0]);
    }

    private static void createFromIndexFilterInt(
            IndexFilter indexFilter,
            ExpressionEvalContext evalContext,
            List<IndexIterationPointer> result
    ) {
        if (indexFilter instanceof IndexRangeFilter) {
            IndexRangeFilter rangeFilter = (IndexRangeFilter) indexFilter;

            Comparable<?> from = null;
            if (rangeFilter.getFrom() != null) {
                Comparable<?> fromValue = rangeFilter.getFrom().getValue(evalContext);
                if (fromValue == null) {
                    return;
                }
                from = fromValue;
            }

            Comparable<?> to = null;
            if (rangeFilter.getTo() != null) {
                Comparable<?> toValue = rangeFilter.getTo().getValue(evalContext);
                if (toValue == null) {
                    return;
                }
                to = toValue;
            }

            result.add(create(from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), false));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            result.add(create(value, true, value, true, false));
        } else if (indexFilter instanceof IndexInFilter) {
            IndexInFilter inFilter = (IndexInFilter) indexFilter;
            for (IndexFilter filter : inFilter.getFilters()) {
                createFromIndexFilterInt(filter, evalContext, result);
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(flags);
        out.writeObject(from);
        if ((flags & FLAG_POINT_LOOKUP) == 0) {
            out.writeObject(to);
        }
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
