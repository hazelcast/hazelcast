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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactSerializer;

import javax.annotation.Nonnull;

/**
 * Tri-tuple that contains the information for the
 * classes and their type names, along with the serializer
 * associated with them.
 */
public class CompactSerializableRegistration {

    private final Class clazz;
    private final CompactSerializer compactSerializer;
    private final String typeName;

    public CompactSerializableRegistration(@Nonnull Class clazz, @Nonnull String typeName,
                                           @Nonnull CompactSerializer compactSerializer) {
        this.clazz = clazz;
        this.typeName = typeName;
        this.compactSerializer = compactSerializer;
    }

    public Class getClazz() {
        return clazz;
    }

    public CompactSerializer getSerializer() {
        return compactSerializer;
    }

    public String getTypeName() {
        return typeName;
    }
}
