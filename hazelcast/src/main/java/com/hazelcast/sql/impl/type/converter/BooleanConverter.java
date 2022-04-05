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

/**
 * Converter for {@link java.lang.Boolean} type.
 */
public final class BooleanConverter extends Converter {

    public static final BooleanConverter INSTANCE = new BooleanConverter();

    static final String TRUE = "true";
    static final String FALSE = "false";

    private BooleanConverter() {
        super(ID_BOOLEAN, QueryDataTypeFamily.BOOLEAN);
    }

    @Override
    public Class<?> getValueClass() {
        return Boolean.class;
    }

    @Override
    public boolean asBoolean(Object val) {
        return ((Boolean) val);
    }

    @Override
    public String asVarchar(Object val) {
        boolean val0 = (Boolean) val;

        return val0 ? TRUE : FALSE;
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asBoolean(val);
    }
}
