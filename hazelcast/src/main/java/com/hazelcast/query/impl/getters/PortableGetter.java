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

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.impl.DefaultPortableReader;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.PortableReader;

final class PortableGetter extends Getter {

    private final InternalSerializationService serializationService;

    public PortableGetter(InternalSerializationService serializationService) {
        super(null);
        this.serializationService = serializationService;
    }

    @Override
    Object getValue(Object target, String fieldPath) throws Exception {
        Data data = (Data) target;
        PortableContext context = serializationService.getPortableContext();
        PortableReader reader = serializationService.createPortableReader(data);
        ClassDefinition classDefinition = context.lookupClassDefinition(data);
        FieldDefinition fieldDefinition = context.getFieldDefinition(classDefinition, fieldPath);

        if (fieldDefinition != null) {
            return ((DefaultPortableReader) reader).read(fieldPath);
        } else {
            return null;
        }
    }

    @Override
    Object getValue(Object obj) throws Exception {
        throw new IllegalArgumentException("Path agnostic value extraction unsupported");
    }

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("Non applicable for PortableGetter");
    }

    @Override
    boolean isCacheable() {
        // Non-cacheable since it's a generic getter and the cache shouldn't be polluted with the same instance
        // for various keys. A singleton should be used instead during getter creation.
        return false;
    }

}
