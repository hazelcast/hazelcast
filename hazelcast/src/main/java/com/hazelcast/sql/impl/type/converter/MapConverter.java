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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.Map;

/**
 * Converter for {@link java.util.Map} type.
 */
public class MapConverter extends Converter {

    public static final MapConverter INSTANCE = new MapConverter();

    public MapConverter() {
        super(ID_MAP, QueryDataTypeFamily.MAP);
    }

    @Override
    public Class<?> getValueClass() {
        return Map.class;
    }

    @Override
    public Object convertToSelf(Converter converter, Object val) {
        Object value = converter.asObject(val);

        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            return value;
        }

        throw converter.cannotConvertError(converter.getTypeFamily());
    }
}
