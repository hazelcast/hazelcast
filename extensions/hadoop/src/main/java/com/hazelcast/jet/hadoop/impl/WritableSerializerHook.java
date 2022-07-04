/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * This class can be extended to quickly create strongly typed serializer hooks for
 * {@code Writable} types. If a class which implements {@code Writable} is not
 * registered explicitly with a {@code SerializerHook} then a default serialization
 * mechanism will be used which fully writes the class name for each item.
 */
public abstract class WritableSerializerHook<T extends Writable> implements SerializerHook<T> {

    private final Class<T> clazz;
    private final Supplier<T> constructor;
    private final int typeId;

    protected WritableSerializerHook(Class<T> clazz, Supplier<T> constructor, int typeId) {
        this.clazz = clazz;
        this.constructor = constructor;
        this.typeId = typeId;
    }

    @Override
    public Class<T> getSerializationType() {
        return clazz;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<Writable>() {

            @Override
            public int getTypeId() {
                return typeId;
            }

            @Override
            public void write(ObjectDataOutput out, Writable writable) throws IOException {
                writable.write(out);
            }

            @Override
            public Writable read(ObjectDataInput in) throws IOException {
                T writable = constructor.get();
                writable.readFields(in);
                return writable;
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
