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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.VersionAwareConstructorFunction;
import com.hazelcast.version.Version;

public final class ArrayDataSerializableFactory implements VersionedDataSerializableFactory {

    private final ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors;
    private final int len;

    public ArrayDataSerializableFactory(ConstructorFunction<Integer, IdentifiedDataSerializable>[] ctorArray) {
        if (ctorArray != null && ctorArray.length > 0) {
            len = ctorArray.length;
            constructors = new ConstructorFunction[len];
            System.arraycopy(ctorArray, 0, constructors, 0, len);
        } else {
            throw new IllegalArgumentException("ConstructorFunction array cannot be null");
        }
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId >= 0 && typeId < len) {
            ConstructorFunction<Integer, IdentifiedDataSerializable> factory = constructors[typeId];
            return factory != null ? factory.createNew(typeId) : null;
        }
        return null;
    }

    @Override
    public IdentifiedDataSerializable create(int typeId,
                                             Version clusterVersion,
                                             Version wanProtocolVersion) {
        if (typeId >= 0 && typeId < len) {
            ConstructorFunction<Integer, IdentifiedDataSerializable> factory = constructors[typeId];
            if (factory == null) {
                return null;
            }
            if (factory instanceof VersionAwareConstructorFunction) {
                return ((VersionAwareConstructorFunction<Integer, IdentifiedDataSerializable>) factory)
                        .createNew(typeId, clusterVersion, wanProtocolVersion);
            } else {
                return factory.createNew(typeId);
            }
        }
        return null;
    }
}
