/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

public class JavaClassQueryTarget implements QueryTarget, GenericTargetAccessor {

    private final String clazzName;
    private final InternalSerializationService serializationService;
    private final Extractors extractors;
    private final boolean isKey;

    private Class<?> clazz;

    private Object target;
    private Object preparedTarget;

    public JavaClassQueryTarget(
        String clazzName,
        InternalSerializationService serializationService,
        Extractors extractors,
        boolean isKey
    ) {
        this.clazzName = clazzName;
        this.serializationService = serializationService;
        this.extractors = extractors;
        this.isKey = isKey;
    }

    @Override
    public void setTarget(Object target) {
        this.target = target;
        this.preparedTarget = null;
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return new GenericTargetExtractor(isKey, this, type);
        } else {
            return new GenericFieldExtractor(isKey, this, type, extractors, path);
        }
    }

    @Override
    public Object getTarget() {
        if (preparedTarget == null) {
            preparedTarget = prepareTarget(target);
        }

        return preparedTarget;
    }

    private Object prepareTarget(Object rawTarget) {
        // Deserialize object if needed.
        Object preparedTarget;

        if (rawTarget instanceof Data) {
            preparedTarget = serializationService.toObject(rawTarget);
        } else {
            preparedTarget = rawTarget;
        }

        // Verify expected class.
        if (clazz == null) {
            Class<?> clazz0 = preparedTarget.getClass();

            if (clazz0.getName().equals(clazzName)) {
                clazz = clazz0;
            } else {
                throw QueryException.dataException("Unexpected value class [expected=" + clazzName
                    + ", actual=" + clazz0.getName() + ']');
            }
        } else {
            if (preparedTarget.getClass() != clazz) {
                throw QueryException.dataException("Unexpected value class [expected=" + clazzName
                    + ", actual=" + preparedTarget.getClass().getName() + ']');
            }
        }

        // Done
        return preparedTarget;
    }
}
