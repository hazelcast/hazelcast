/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

/**
 * Complementary interface to {@link Converter} that converts values back to
 * the class returned by {@link Converter#getValueClass()}.
 */
public abstract class ToConverter {
    // Normalized value class is not necessarily assignable from value class.
    // For example, normalized value class of BigInteger is BigDecimal, which
    // is not a superclass of it.
    private final Class<?> valueClass;
    private final Class<?> normalizedValueClass;

    protected ToConverter(QueryDataType type) {
        valueClass = type.getConverter().getValueClass();
        normalizedValueClass = type.getConverter().getNormalizedValueClass();
    }

    public Object convert(Object value) {
        if (value == null || valueClass.isInstance(value)) {
            return value;
        }

        if (!normalizedValueClass.isInstance(value)) {
            throw QueryException.dataException("Type mismatch [expectedClass=" + normalizedValueClass.getName()
                    + ", actualClass=" + value.getClass().getName() + "]");
        }

        return from(value);
    }

    protected abstract Object from(Object canonicalValue);
}
