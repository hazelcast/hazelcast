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

package com.hazelcast.sql.impl.row;

import com.hazelcast.sql.impl.exec.KeyValueRowExtractor;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.util.Arrays;
import java.util.List;

/**
 * Key-value row. Appears during iteration over a data stored in map or its index.
 */
public final class KeyValueRow implements Row {

    private static final Object NULL = new Object();

    private final List<String> fieldNames;
    private final List<QueryDataType> fieldTypes;
    private final KeyValueRowExtractor extractor;

    private final Object[] cachedColumnValues;
    private final Class<?>[] cachedColumnClasses;
    private final Converter[] cachedColumnConverters;

    private Object key;
    private Object value;

    public KeyValueRow(List<String> fieldNames, List<QueryDataType> fieldTypes, KeyValueRowExtractor extractor) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.extractor = extractor;

        cachedColumnValues = new Object[fieldNames.size()];
        cachedColumnClasses = new Class[fieldNames.size()];
        cachedColumnConverters = new Converter[fieldNames.size()];
    }

    public void setKeyValue(Object key, Object val) {
        this.key = key;
        this.value = val;

        Arrays.fill(cachedColumnValues, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getColumn(int index) {
        Object columnValue = cachedColumnValues[index];

        if (columnValue == null) {
            columnValue = extractor.extract(key, value, fieldNames.get(index));
            if (columnValue != null) {
                columnValue = convert(index, columnValue);
            }

            cachedColumnValues[index] = columnValue == null ? NULL : columnValue;
        } else if (columnValue == NULL) {
            columnValue = null;
        }

        return (T) columnValue;
    }

    @Override
    public int getColumnCount() {
        return fieldNames.size();
    }

    private Object convert(int index, Object columnValue) {
        Converter converter;

        Class<?> clazz = columnValue.getClass();
        if (clazz == cachedColumnClasses[index]) {
            converter = cachedColumnConverters[index];
        } else {
            converter = Converters.getConverter(clazz);

            cachedColumnClasses[index] = clazz;
            cachedColumnConverters[index] = converter;
        }

        return fieldTypes.get(index).getConverter().convertToSelf(converter, columnValue);
    }

}
