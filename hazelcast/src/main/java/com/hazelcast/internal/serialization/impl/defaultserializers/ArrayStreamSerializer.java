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

package com.hazelcast.internal.serialization.impl.defaultserializers;

import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * The {@link Object[]} serializer
 */
public class ArrayStreamSerializer implements StreamSerializer<Object[]> {
    @Override
    public void write(ObjectDataOutput out, Object[] object) throws IOException {
        out.writeInt(object.length);
        for (int i = 0; i < object.length; i++) {
            out.writeObject(object[i]);
        }
    }

    @Override
    public Object[] read(ObjectDataInput in) throws IOException {
        int length = in.readInt();
        Object[] objects = new Object[length];
        for (int i = 0; i < length; i++) {
            objects[i] = in.readObject();
        }
        return objects;
    }

    @Override
    public void destroy() {
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_ARRAY;
    }
}
