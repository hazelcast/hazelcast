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

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;

public final class PortableGetter extends Getter {
    private final InternalSerializationService serializationService;

    public PortableGetter(InternalSerializationService serializationService) {
        super(null);
        this.serializationService = serializationService;
    }

    private Object getValueInternal(Object target, String fieldPath, boolean useLazyDeserialization) throws Exception {
        InternalGenericRecord record;
        if (target instanceof PortableGenericRecord) {
            record = (InternalGenericRecord) target;
        } else {
            record = serializationService.readAsInternalGenericRecord((Data) target);
        }
        GenericRecordQueryReader reader = new GenericRecordQueryReader(record, useLazyDeserialization);
        return reader.read(fieldPath);
    }

    @Override
    public Object getValue(Object target, String fieldPath) throws Exception {
        return getValueInternal(target, fieldPath, false);
    }

    @Override
    public Object getValue(Object obj, String attributePath, Object metadata) throws Exception {
        if (metadata == null) {
            return getValue(obj, attributePath);
        }
        boolean useLazyDeserialization = (boolean) metadata;
        return getValueInternal(obj, attributePath, useLazyDeserialization);
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
