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
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * A test batch-data connector. It emits rows of provided types and values.
 * It emits a slice of the rows on each member.
 */
public class TestBatchSqlConnector extends TestAbstractSqlConnector {

    static final String TYPE_NAME = "TestBatch";

    /**
     * Creates a table with single column named "v" with INT type.
     * The rows contain the sequence {@code 0 .. itemCount}.
     */
    public static void create(SqlService sqlService, String tableName, int itemCount) {
        create(sqlService, tableName, itemCount, false);
    }

    /**
     * Creates a table with single column named "v" with INT type.
     * The rows contain the sequence {@code 0 .. itemCount}.
     *
     * @param behaveLikeAStream If true, the source will emit the rows, but then it will not complete, which
     *                          will give stream-like behavior.
     */
    public static void create(SqlService sqlService, String tableName, int itemCount, boolean behaveLikeAStream) {
        List<String[]> values = IntStream.range(0, itemCount)
                .mapToObj(i -> new String[]{String.valueOf(i)})
                .collect(toList());
        create(sqlService, TYPE_NAME, tableName, singletonList("v"), singletonList(QueryDataTypeFamily.INTEGER), values,
                behaveLikeAStream);
    }

    public static List<String[]> valuesToString(Object[]... values) {
        return stream(values)
                .map(row -> stream(row).map(value -> value == null ? null : value.toString()).toArray(String[]::new))
                .collect(Collectors.toList());
    }

    public static void create(
            SqlService sqlService,
            String tableName,
            List<String> names,
            List<QueryDataTypeFamily> types,
            List<String[]> values
    ) {
        TestAbstractSqlConnector.create(sqlService, TYPE_NAME, tableName, names, types, values, false);
    }

    @Override
    protected ProcessorMetaSupplier createProcessorSupplier(FunctionEx<Context, TestDataGenerator> createContextFn) {
        BatchSource<Object> source = SourceBuilder
                .batch("batch", createContextFn)
                .fillBufferFn(TestDataGenerator::fillBuffer)
                .build();
        return  ((BatchSourceTransform<Object>) source).metaSupplier;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
    }
}
