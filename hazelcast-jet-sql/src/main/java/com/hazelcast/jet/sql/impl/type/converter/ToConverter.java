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

package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

/**
 * Complementary interface to {@link Converter} that converts values back to
 * the class returned by {@link Converter#getValueClass()}.
 */
public abstract class ToConverter {

    private final Class<?> normalizedValueClass;

    protected ToConverter(QueryDataType type) {
        this.normalizedValueClass = type.getConverter().getNormalizedValueClass();
    }

    public Object convert(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> valueClass = value.getClass();

        if (!normalizedValueClass.isAssignableFrom(valueClass)) {
            throw QueryException.dataException("Type mismatch [expectedClass=" + normalizedValueClass.getName()
                    + ", actualClass=" + valueClass.getName() + ']');
        }

        return from(value);
    }

    protected abstract Object from(Object canonicalValue);
}
