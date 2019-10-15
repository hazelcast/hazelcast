/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Represents a partial query result on a segment of the map.
 * The remaining query results may be retrieved using
 * the {@link #nextTableIndexToReadFrom} which signifies the next query result.
 */
public class ResultSegment implements IdentifiedDataSerializable {
    private Result result;
    private int nextTableIndexToReadFrom;

    public ResultSegment() {
    }

    public ResultSegment(Result result, int nextTableIndexToReadFrom) {
        this.result = result;
        this.nextTableIndexToReadFrom = nextTableIndexToReadFrom;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public int getNextTableIndexToReadFrom() {
        return nextTableIndexToReadFrom;
    }

    public void setNextTableIndexToReadFrom(int nextTableIndexToReadFrom) {
        this.nextTableIndexToReadFrom = nextTableIndexToReadFrom;
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
        out.writeInt(nextTableIndexToReadFrom);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        result = in.readObject();
        nextTableIndexToReadFrom = in.readInt();
    }
}
