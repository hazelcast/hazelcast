/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializer.HasSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * {@code IdentifiedDataSerializer} makes it possible to define a custom
 * {@link Serializer} for an existing {@link IdentifiedDataSerializable}.
 * The object that needs custom serialization should implement {@link
 * HasSerializer HasSerializer} and provide its serializer via {@link
 * HasSerializer#getSerializer() getSerializer()}. Using {@code
 * IdentifiedDataSerializer}, objects of subclasses can be (de)serialized
 * without extra configuration, and Java {@link Serializable}'s {@code
 * readResolve()} functionality can be emulated without creating temporary
 * objects.
 *
 * @since 5.4
 */
public interface IdentifiedDataSerializer<T extends HasSerializer<T>>
        extends IdentifiedDataSerializable, StreamSerializer<T> {

    @Override
    default int getTypeId() {
        return getClassId();
    }

    @Override
    default void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " is a serializer");
    }

    @Override
    default void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " is a serializer");
    }

    interface HasSerializer<T extends HasSerializer<T>> extends IdentifiedDataSerializable {

        IdentifiedDataSerializer<T> getSerializer();

        @Override
        default int getFactoryId() {
            return getSerializer().getFactoryId();
        }

        @Override
        default int getClassId() {
            return getSerializer().getClassId();
        }

        @Override
        @SuppressWarnings("unchecked")
        default void writeData(ObjectDataOutput out) throws IOException {
            getSerializer().write(out, (T) this);
        }

        @Override
        default void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException(getClass().getName() + " has a serializer");
        }
    }
}
