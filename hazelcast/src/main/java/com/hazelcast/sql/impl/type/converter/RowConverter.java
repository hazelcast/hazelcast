/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.type.RowTypeMarker;

public class RowConverter extends Converter {
    public static final RowConverter INSTANCE = new RowConverter();

    public RowConverter() {
        super(ID_ROW, QueryDataTypeFamily.ROW);
    }

    @Override
    public Class<?> getValueClass() {
        return RowTypeMarker.class;
    }

    @Override
    public Object convertToSelf(final Converter converter, final Object val) {
        return converter.asRow(val);
    }

    @Override
    public String asVarchar(final Object val) {
        return val.toString();
    }

    @Override
    public RowTypeMarker asRow(final Object val) {
        return (RowTypeMarker) val;
    }
}
