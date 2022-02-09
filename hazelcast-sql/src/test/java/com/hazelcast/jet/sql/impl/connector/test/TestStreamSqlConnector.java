/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * A test stream-data connector. It emits rows of provided types and values.
 */
public class TestStreamSqlConnector extends TestAbstractSqlConnector {

    public static final String TYPE_NAME = "TestStream";

    public static void create(
            SqlService sqlService,
            String tableName,
            List<String> names,
            List<QueryDataTypeFamily> types,
            Object[]... values
    ) {
        List<String[]> stringValues = stream(values)
                .map(row -> stream(row).map(value -> value == null ? null : value.toString()).toArray(String[]::new))
                .collect(Collectors.toList());
        TestAbstractSqlConnector.create(sqlService, TYPE_NAME, tableName, names, types, stringValues, true);
    }

    @Override
    protected ProcessorMetaSupplier createProcessorSupplier(FunctionEx<Context, TestDataGenerator> createContextFn) {
        StreamSource<Object> source = SourceBuilder
                .stream("stream", createContextFn)
                .fillBufferFn(TestDataGenerator::fillBuffer)
                .build();
        return ((StreamSourceTransform<Object>) source).metaSupplierFn.apply(null);
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }
}
