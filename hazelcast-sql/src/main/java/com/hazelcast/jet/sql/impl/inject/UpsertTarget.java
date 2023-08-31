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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;

public abstract class UpsertTarget {

    protected <T> Injector<T> createRecordInjector(QueryDataType type,
                                                   BiFunction<String, QueryDataType, Injector<T>> createFieldInjector) {
        List<Injector<T>> injectors = type.getObjectFields().stream()
                .map(field -> createFieldInjector.apply(field.getName(), field.getDataType()))
                .collect(toList());
        return (record, value) -> {
            for (int i = 0; i < injectors.size(); i++) {
                injectors.get(i).set(record, ((RowValue) value).getValues().get(i));
            }
        };
    }

    public abstract UpsertInjector createInjector(@Nullable String path, QueryDataType type);

    public abstract void init();

    public abstract Object conclude();

    @FunctionalInterface
    protected interface Injector<T> {
        void set(T record, Object value);
    }
}
