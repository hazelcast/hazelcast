/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.ArrayList;

import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * The {@link java.util.ArrayList} serializer
 */
public class ArrayListStreamSerializer implements StreamSerializer<ArrayList> {

    @Override
    public void write(ObjectDataOutput out, ArrayList arrayList) throws IOException {
        int size = arrayList == null ? NULL_ARRAY_LENGTH : arrayList.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeObject(arrayList.get(i));
        }
    }

    @Override
    public ArrayList read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        ArrayList result = null;
        if (size > NULL_ARRAY_LENGTH) {
            result = new ArrayList(size);
            for (int i = 0; i < size; i++) {
                result.add(i, in.readObject());
            }
        }
        return result;
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_ARRAY_LIST;
    }

    @Override
    public void destroy() {
    }
}
