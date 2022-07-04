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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.LazyTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

public class GenericQueryTarget implements QueryTarget, GenericTargetAccessor {

    private final InternalSerializationService serializationService;
    private final Extractors extractors;
    private final boolean key;

    private Object deserialized;
    private Data serialized;

    private LazyTarget targetWithObjectTypeForDirectAccess;
    private Object targetForFieldAccess;

    public GenericQueryTarget(InternalSerializationService serializationService, Extractors extractors, boolean key) {
        this.serializationService = serializationService;
        this.extractors = extractors;
        this.key = key;
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        serialized = targetData;
        deserialized = target;

        targetWithObjectTypeForDirectAccess = null;
        targetForFieldAccess = null;
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return new GenericTargetExtractor(key, this, type);
        } else {
            return new GenericFieldExtractor(key, this, type, extractors, path);
        }
    }

    @Override
    public Object getTargetForFieldAccess() {
        if (targetForFieldAccess == null) {
            targetForFieldAccess = prepareTargetForFieldAccess();
        }

        return targetForFieldAccess;
    }

    /**
     * Get target that should be used for field access.
     *
     * @return serialized form for {@link Portable}/Compact (see {@link CompactSerializationConfig}),
     * deserialized form otherwise
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    private Object prepareTargetForFieldAccess() {
        if (targetWithObjectTypeForDirectAccess != null) {
            // If a target for direct access is already initialized, get data from it.
            deserialized = targetWithObjectTypeForDirectAccess.getDeserialized();
            serialized = targetWithObjectTypeForDirectAccess.getSerialized();
        }

        if (deserialized != null) {
            if (deserialized instanceof Portable || serializationService.isCompactSerializable(deserialized)) {
                // Serialize Portable/Compact to Data.
                if (serialized == null) {
                    serialized = serializationService.toData(deserialized);

                    if (targetWithObjectTypeForDirectAccess != null) {
                        targetWithObjectTypeForDirectAccess.setSerialized(serialized);
                    }
                }

                return serialized;
            } else {
                // Return deserialized object.
                return deserialized;
            }
        } else {
            assert serialized != null;

            if (serialized.isPortable() || serialized.isCompact()) {
                // Return Portable/Compact as Data.
                return serialized;
            } else {
                // Deserialize otherwise.
                if (deserialized == null) {
                    deserialized = serializationService.toObject(serialized);

                    if (targetWithObjectTypeForDirectAccess != null) {
                        targetWithObjectTypeForDirectAccess.setDeserialized(deserialized);
                    }
                }

                return deserialized;
            }
        }
    }

    @Override
    public Object getTargetForDirectAccess(QueryDataType type) {
        if (type.getTypeFamily() != QueryDataTypeFamily.OBJECT) {
            // For the built-in types, we always work with the deserialized representation.
            if (deserialized == null) {
                deserialized = serializationService.toObject(this.serialized);
            }

            return type.normalize(deserialized);
        } else {
            // For the OBJECT type, we try to delay the deserialization for as long as possible.
            if (targetWithObjectTypeForDirectAccess == null) {
                targetWithObjectTypeForDirectAccess = new LazyTarget(serialized, deserialized);
            }

            return targetWithObjectTypeForDirectAccess;
        }
    }

    public boolean isKey() {
        return key;
    }
}
