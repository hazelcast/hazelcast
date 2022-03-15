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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for a cursor class holding a collection of items and the
 * pointers representing the items already traversed. These pointers
 * are a compact way to represent the current iteration state from which
 * iteration may be resumed.
 *
 * @param <T> the type of item being iterated
 */
public abstract class AbstractCursor<T> implements IdentifiedDataSerializable {
    private List<T> objects;
    private IterationPointer[] pointers;

    public AbstractCursor() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public AbstractCursor(List<T> entries, IterationPointer[] pointers) {
        this.objects = entries;
        this.pointers = pointers;
    }

    public List<T> getBatch() {
        return objects;
    }

    /**
     * Returns the iteration pointers representing the current iteration state.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is an internal class")
    public IterationPointer[] getIterationPointers() {
        return pointers;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    abstract void writeElement(ObjectDataOutput out, T element) throws IOException;

    abstract T readElement(ObjectDataInput in) throws IOException;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(pointers.length);
        for (IterationPointer pointer : pointers) {
            out.writeInt(pointer.getIndex());
            out.writeInt(pointer.getSize());
        }
        out.writeInt(objects.size());
        for (T entry : objects) {
            writeElement(out, entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int pointersCount = in.readInt();
        pointers = new IterationPointer[pointersCount];
        for (int i = 0; i < pointersCount; i++) {
            pointers[i] = new IterationPointer(in.readInt(), in.readInt());
        }
        int size = in.readInt();
        objects = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            objects.add(readElement(in));
        }
    }
}
