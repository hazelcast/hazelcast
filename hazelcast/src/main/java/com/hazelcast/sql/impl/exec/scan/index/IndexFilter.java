/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;

/**
 * Index filter which is transferred over a wire.
 */
// TODO: Use IDS
public class IndexFilter implements DataSerializable {

    private IndexFilterType type;
    private Expression from;
    private boolean fromInclusive;
    private Expression to;
    private boolean toInclusive;

    public IndexFilter() {
        // No-op.
    }

    private IndexFilter(IndexFilterType type, Expression from, boolean fromInclusive, Expression to, boolean toInclusive) {
        this.type = type;
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
    }

    public static IndexFilter forEquals(Expression value) {
        return new IndexFilter(IndexFilterType.EQUALS, value, false, null, false);
    }

    public static IndexFilter forIn(Expression value) {
        return new IndexFilter(IndexFilterType.IN, value, false, null, false);
    }

    public static IndexFilter forRange(Expression from, boolean fromInclusive, Expression to, boolean toInclusive) {
        return new IndexFilter(IndexFilterType.RANGE, from, fromInclusive, to, toInclusive);
    }

    public IndexFilterType getType() {
        return type;
    }

    public Expression getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return fromInclusive;
    }

    public Expression getTo() {
        return to;
    }

    public boolean isToInclusive() {
        return toInclusive;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getId());
        out.writeObject(from);
        out.writeBoolean(fromInclusive);
        out.writeObject(to);
        out.writeBoolean(toInclusive);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = IndexFilterType.getById(in.readInt());
        from = in.readObject();
        fromInclusive = in.readBoolean();
        to = in.readObject();
        toInclusive = in.readBoolean();
    }
}
