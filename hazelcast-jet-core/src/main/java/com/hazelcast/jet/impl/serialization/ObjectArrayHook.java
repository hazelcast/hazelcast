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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public final class ObjectArrayHook implements SerializerHook<Object[]> {

    @Override
    public Class<Object[]> getSerializationType() {
        return Object[].class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<Object[]>() {

            @Override
            public int getTypeId() {
                return SerializerHookConstants.OBJECT_ARRAY;
            }

            @Override
            public void destroy() {

            }

            @Override
            public void write(ObjectDataOutput out, Object[] array) throws IOException {
                out.writeInt(array.length);
                for (int i = 0; i < array.length; i++) {
                    out.writeObject(array[i]);
                }
            }

            @Override
            public Object[] read(ObjectDataInput in) throws IOException {
                int length = in.readInt();
                Object[] array = new Object[length];
                for (int i = 0; i < array.length; i++) {
                    array[i] = in.readObject();
                }
                return array;
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
