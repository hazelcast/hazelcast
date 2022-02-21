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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Represents a partial query result on a segment of the map.
 * The remaining query results may be retrieved using the {@link #pointers}
 * which defines the iteration state.
 */
public class ResultSegment implements IdentifiedDataSerializable {
    private Result result;
    private IterationPointer[] pointers;

    public ResultSegment() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public ResultSegment(Result result, IterationPointer[] pointers) {
        this.result = result;
        this.pointers = pointers;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    /**
     * Returns the iteration pointers representing the current iteration state.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is an internal class")
    public IterationPointer[] getPointers() {
        return pointers;
    }

    /**
     * Sets the iteration pointers representing the current iteration state.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public void setPointers(IterationPointer[] pointers) {
        this.pointers = pointers;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.RESULT_SEGMENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(result);
        out.writeInt(pointers.length);
        for (IterationPointer pointer : pointers) {
            out.writeInt(pointer.getIndex());
            out.writeInt(pointer.getSize());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        result = in.readObject();
        int pointersCount = in.readInt();
        pointers = new IterationPointer[pointersCount];
        for (int i = 0; i < pointersCount; i++) {
            pointers[i] = new IterationPointer(in.readInt(), in.readInt());
        }
    }
}
