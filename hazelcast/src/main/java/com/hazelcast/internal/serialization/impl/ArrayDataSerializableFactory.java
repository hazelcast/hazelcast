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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.Supplier;

public final class ArrayDataSerializableFactory implements DataSerializableFactory {

    private final Supplier<IdentifiedDataSerializable>[] constructors;
    private final int len;

    public ArrayDataSerializableFactory(Supplier<IdentifiedDataSerializable>[] ctorArray) {
        if (ctorArray != null && ctorArray.length > 0) {
            len = ctorArray.length;
            constructors = new Supplier[len];
            System.arraycopy(ctorArray, 0, constructors, 0, len);
        } else {
            throw new IllegalArgumentException("ConstructorFunction array cannot be null");
        }
    }

    @Override
    @Nullable
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId >= 0 && typeId < len) {
            Supplier<IdentifiedDataSerializable> factory = constructors[typeId];
            return factory != null ? factory.get() : null;
        }
        return null;
    }

    public void mergeConstructors(@Nonnull Supplier<IdentifiedDataSerializable>[] ctorArray) {
        if (constructors.length < ctorArray.length) {
            throw new IllegalArgumentException("Too many constructors");
        }
        for (int i = 0; i < ctorArray.length; i++) {
            if (ctorArray[i] != null) {
                if (constructors[i] != null) {
                    throw new IllegalArgumentException("Overwriting a constructor");
                }
                constructors[i] = ctorArray[i];
            }
        }
    }
}
