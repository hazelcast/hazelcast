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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;

public abstract class AbstractDecimalConverter extends Converter {
    protected AbstractDecimalConverter(int id) {
        super(id, QueryDataTypeFamily.DECIMAL);
    }

    @Override
    public Class<?> getNormalizedValueClass() {
        return BigDecimal.class;
    }

    @Override
    public final Object asObject(Object val) {
        return asDecimal(val);
    }

    @Override
    public final Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asDecimal(val);
    }
}
