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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class SampleIdentifiedDataSerializable implements IdentifiedDataSerializable {

    private char c;
    private int i;

    public SampleIdentifiedDataSerializable(char c, int i) {
        this.c = c;
        this.i = i;
    }

    public SampleIdentifiedDataSerializable() {
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.DATA_SERIALIZABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.SAMPLE_IDENTIFIED_DATA_SERIALIZABLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(i);
        out.writeChar(c);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        i = in.readInt();
        c = in.readChar();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SampleIdentifiedDataSerializable that = (SampleIdentifiedDataSerializable) o;

        if (c != that.c) {
            return false;
        }
        return i == that.i;
    }

    @Override
    public int hashCode() {
        int result = (int) c;
        result = 31 * result + i;
        return result;
    }
}
