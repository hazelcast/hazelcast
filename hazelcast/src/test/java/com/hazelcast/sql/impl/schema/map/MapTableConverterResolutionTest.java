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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.impl.CompositeConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.query.impl.TypeConverters.BIG_DECIMAL_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.BIG_INTEGER_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.BOOLEAN_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.BYTE_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.CHAR_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.DATE_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.DOUBLE_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.ENUM_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.FLOAT_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.LONG_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.PORTABLE_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.SHORT_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.SQL_DATE_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.SQL_TIMESTAMP_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.STRING_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.UUID_CONVERTER;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.isCompatibleForIndexRequest;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR_CHARACTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapTableConverterResolutionTest {

    @Test
    public void testSimpleConverters() {
        checkSimpleConverter(BOOLEAN_CONVERTER, BOOLEAN);

        checkSimpleConverter(BYTE_CONVERTER, TINYINT);
        checkSimpleConverter(SHORT_CONVERTER, SMALLINT);
        checkSimpleConverter(INTEGER_CONVERTER, INT);
        checkSimpleConverter(LONG_CONVERTER, BIGINT);

        checkSimpleConverter(BIG_INTEGER_CONVERTER, DECIMAL_BIG_INTEGER);
        checkSimpleConverter(BIG_DECIMAL_CONVERTER, DECIMAL);

        checkSimpleConverter(FLOAT_CONVERTER, REAL);
        checkSimpleConverter(DOUBLE_CONVERTER, DOUBLE);

        checkSimpleConverter(CHAR_CONVERTER, VARCHAR_CHARACTER);
        checkSimpleConverter(STRING_CONVERTER, VARCHAR);

        checkSimpleConverter(ENUM_CONVERTER, OBJECT);
        checkSimpleConverter(IDENTITY_CONVERTER, OBJECT);
        checkSimpleConverter(PORTABLE_CONVERTER, OBJECT);
        checkSimpleConverter(UUID_CONVERTER, OBJECT);

        checkUnsupportedConverter(DATE_CONVERTER);
        checkUnsupportedConverter(SQL_DATE_CONVERTER);
        checkUnsupportedConverter(SQL_TIMESTAMP_CONVERTER);
        checkUnsupportedConverter(NULL_CONVERTER);
    }

    private void checkSimpleConverter(TypeConverter converter, QueryDataType expectedType) {
        assertEquals(expectedType, MapTableUtils.indexConverterToSqlType(converter));
        assertEquals(Collections.singletonList(expectedType), MapTableUtils.indexConverterToSqlTypes(converter));
    }

    private void checkUnsupportedConverter(TypeConverter converter) {
        assertNull(MapTableUtils.indexConverterToSqlType(converter));
        assertEquals(Collections.emptyList(), MapTableUtils.indexConverterToSqlTypes(converter));

        CompositeConverter badGood = new CompositeConverter(new TypeConverter[] { converter, INTEGER_CONVERTER });
        assertEquals(Collections.emptyList(), MapTableUtils.indexConverterToSqlTypes(badGood));

        CompositeConverter goodBad = new CompositeConverter(new TypeConverter[] { INTEGER_CONVERTER, converter });
        assertEquals(Collections.singletonList(INT), MapTableUtils.indexConverterToSqlTypes(goodBad));
    }

    @Test
    public void testCompositeConverters() {
        checkCompositeConverters(BOOLEAN_CONVERTER, BOOLEAN);

        checkCompositeConverters(BYTE_CONVERTER, TINYINT);
        checkCompositeConverters(SHORT_CONVERTER, SMALLINT);
        checkCompositeConverters(INTEGER_CONVERTER, INT);
        checkCompositeConverters(LONG_CONVERTER, BIGINT);

        checkCompositeConverters(BIG_INTEGER_CONVERTER, DECIMAL_BIG_INTEGER);
        checkCompositeConverters(BIG_DECIMAL_CONVERTER, DECIMAL);

        checkCompositeConverters(FLOAT_CONVERTER, REAL);
        checkCompositeConverters(DOUBLE_CONVERTER, DOUBLE);

        checkCompositeConverters(CHAR_CONVERTER, VARCHAR_CHARACTER);
        checkCompositeConverters(STRING_CONVERTER, VARCHAR);

        checkCompositeConverters(ENUM_CONVERTER, OBJECT);
        checkCompositeConverters(IDENTITY_CONVERTER, OBJECT);
        checkCompositeConverters(PORTABLE_CONVERTER, OBJECT);
        checkCompositeConverters(UUID_CONVERTER, OBJECT);
    }

    private void checkCompositeConverters(TypeConverter converter1, QueryDataType expectedType) {
        checkCompositeConverter(converter1, BOOLEAN_CONVERTER, expectedType, BOOLEAN);

        checkCompositeConverter(converter1, BYTE_CONVERTER, expectedType, TINYINT);
        checkCompositeConverter(converter1, SHORT_CONVERTER, expectedType, SMALLINT);
        checkCompositeConverter(converter1, INTEGER_CONVERTER, expectedType, INT);
        checkCompositeConverter(converter1, LONG_CONVERTER, expectedType, BIGINT);

        checkCompositeConverter(converter1, BIG_INTEGER_CONVERTER, expectedType, DECIMAL_BIG_INTEGER);
        checkCompositeConverter(converter1, BIG_DECIMAL_CONVERTER, expectedType, DECIMAL);

        checkCompositeConverter(converter1, FLOAT_CONVERTER, expectedType, REAL);
        checkCompositeConverter(converter1, DOUBLE_CONVERTER, expectedType, DOUBLE);

        checkCompositeConverter(converter1, CHAR_CONVERTER, expectedType, VARCHAR_CHARACTER);
        checkCompositeConverter(converter1, STRING_CONVERTER, expectedType, VARCHAR);

        checkCompositeConverter(converter1, ENUM_CONVERTER, expectedType, OBJECT);
        checkCompositeConverter(converter1, IDENTITY_CONVERTER, expectedType, OBJECT);
        checkCompositeConverter(converter1, PORTABLE_CONVERTER, expectedType, OBJECT);
        checkCompositeConverter(converter1, UUID_CONVERTER, expectedType, OBJECT);
    }

    private void checkCompositeConverter(TypeConverter converter1, TypeConverter converter2, QueryDataType... expectedTypes) {
        CompositeConverter converter = new CompositeConverter(new TypeConverter[] { converter1, converter2 });

        List<QueryDataType> types = MapTableUtils.indexConverterToSqlTypes(converter);
        List<QueryDataType> expectedTypes0 = expectedTypes == null ? Collections.emptyList() : Arrays.asList(expectedTypes);

        assertEquals(expectedTypes0, types);
    }

    @Test
    public void testConverterCompatibility() {
        // Test compatibility
        checkConverterCompatibility(
            BOOLEAN
        );

        checkConverterCompatibilityNumeric(TINYINT);
        checkConverterCompatibilityNumeric(SMALLINT);
        checkConverterCompatibilityNumeric(INT);
        checkConverterCompatibilityNumeric(BIGINT);

        checkConverterCompatibilityNumeric(DECIMAL);
        checkConverterCompatibilityNumeric(DECIMAL_BIG_INTEGER);

        checkConverterCompatibilityNumeric(REAL);
        checkConverterCompatibilityNumeric(DOUBLE);

        checkConverterCompatibility(
            VARCHAR,
            VARCHAR_CHARACTER
        );

        checkConverterCompatibility(
            VARCHAR_CHARACTER,
            VARCHAR
        );

        checkConverterCompatibility(
            OBJECT
        );
    }

    private void checkConverterCompatibilityNumeric(QueryDataType columnType) {
        checkConverterCompatibility(
            columnType,
            TINYINT, SMALLINT, INT, BIGINT, DECIMAL_BIG_INTEGER, DECIMAL, REAL, DOUBLE
        );
    }

    private void checkConverterCompatibility(QueryDataType columnType, QueryDataType... supportedIndexConverterTypes) {
        Set<QueryDataType> supportedIndexConverterTypes0 = supportedIndexConverterTypes == null
            ? new HashSet<>() : new HashSet<>(Arrays.asList(supportedIndexConverterTypes));

        // Always compatible with self
        supportedIndexConverterTypes0.add(columnType);

        checkConverterCompatibility(columnType, BOOLEAN, supportedIndexConverterTypes0);

        checkConverterCompatibility(columnType, TINYINT, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, SMALLINT, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, INT, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, BIGINT, supportedIndexConverterTypes0);

        checkConverterCompatibility(columnType, DECIMAL, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, DECIMAL_BIG_INTEGER, supportedIndexConverterTypes0);

        checkConverterCompatibility(columnType, REAL, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, DOUBLE, supportedIndexConverterTypes0);

        checkConverterCompatibility(columnType, VARCHAR, supportedIndexConverterTypes0);
        checkConverterCompatibility(columnType, VARCHAR_CHARACTER, supportedIndexConverterTypes0);

        checkConverterCompatibility(columnType, OBJECT, supportedIndexConverterTypes0);
    }

    private void checkConverterCompatibility(QueryDataType columnType, QueryDataType indexConverterType, Set<QueryDataType> supportedIndexConverterTypes) {
        boolean supported = supportedIndexConverterTypes.contains(indexConverterType);

        assertEquals(supported, isCompatibleForIndexRequest(columnType, indexConverterType));
    }
}
