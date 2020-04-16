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

import java.io.IOException;

/**
 * Index filter which is transferred over a wire.
 */
public class IndexFilter implements DataSerializable {
    /** Condition type. */
    private IndexFilterType type;

    /** Value. */
    private Object value;

    public IndexFilter() {
        // No-op.
    }

    public IndexFilter(IndexFilterType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public IndexFilterType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getId());
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = IndexFilterType.getById(in.readInt());
        value = in.readObject();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{type=" + type + ", value=" + value + '}';
    }
}
