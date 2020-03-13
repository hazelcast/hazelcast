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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

/**
 * Converter for arbitrary objects which do not have more specific converter.
 */
public final class ObjectConverter extends AbstractObjectConverter {

    public static final ObjectConverter INSTANCE = new ObjectConverter();

    private ObjectConverter() {
        super(ID_OBJECT, QueryDataTypeFamily.OBJECT);
    }

    @Override
    public Class<?> getValueClass() {
        return Object.class;
    }

    @Override
    public String asVarchar(Object val) {
        Converter converter = Converters.getConverter(val.getClass());

        if (converter == this) {
            return val.toString();
        } else {
            return converter.asVarchar(val);
        }
    }

    @Override
    public Object asObject(Object val) {
        Converter converter = Converters.getConverter(val.getClass());

        if (converter == this) {
            return val;
        } else {
            return converter.asObject(val);
        }
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asObject(val);
    }
}
