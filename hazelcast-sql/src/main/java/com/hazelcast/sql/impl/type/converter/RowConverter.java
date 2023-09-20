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

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

/**
 * Converter for {@link RowValue} type.
 */
@SerializableByConvention
public class RowConverter extends Converter {
    public static final RowConverter INSTANCE = new RowConverter();

    protected RowConverter() {
        super(ID_ROW, QueryDataTypeFamily.ROW);
    }

    @Override
    public Class<?> getValueClass() {
        return RowValue.class;
    }

    @Override
    public Object convertToSelf(final Converter converter, final Object val) {
        return converter.asRow(val);
    }

    @Override
    public String asVarchar(Object val) {
        return val.toString();
    }
}
