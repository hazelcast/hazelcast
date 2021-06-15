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
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexIterationPointer implements IdentifiedDataSerializable {

    private static final int FLAG_DESCENDING = 1;
    private static final int FLAG_FROM_INCLUSIVE = 2;
    private static final int FLAG_TO_INCLUSIVE = 4;

    private Comparable<?> from;
    private Comparable<?> to;
    private byte flags;
    private boolean done;

    public IndexIterationPointer() {
    }

    public IndexIterationPointer(
            Comparable<?> from,
            boolean fromInclusive,
            Comparable<?> to,
            boolean toInclusive,
            boolean descending
    ) {
        this.from = from;
        this.to = to;

        flags = (byte) ((descending ? FLAG_DESCENDING : 0)
                | (fromInclusive ? FLAG_FROM_INCLUSIVE : 0)
                | (toInclusive ? FLAG_TO_INCLUSIVE : 0));
    }

    private IndexIterationPointer(boolean done) {
        this.done = done;
    }

    public static IndexIterationPointer createFinishedIterator() {
        return new IndexIterationPointer(true);
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
            Comparable<?> from = rangeFilter.getFrom().getValue(evalContext);
            Comparable<?> to = rangeFilter.getTo().getValue(evalContext);
            result.add(new IndexIterationPointer(from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), false));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            result.add(new IndexIterationPointer(value, true, value, true, false));
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

    public boolean isDone() {
        return done;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(from);
        out.writeObject(to);
        out.writeByte(flags);
        out.writeBoolean(done);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        from = in.readObject();
        to = in.readObject();
        flags = in.readByte();
        done = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return 0;
    }
}
