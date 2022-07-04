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

package com.hazelcast.jet.sql.impl.validate.types;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isObjectIdentifier;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.OTHER;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastTypeSystemTest {

    private static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    @Test
    public void numericPrecisionAndScaleTest() {
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.MAX_DECIMAL_PRECISION);
        assertEquals(QueryDataType.MAX_DECIMAL_SCALE, HazelcastTypeSystem.MAX_DECIMAL_SCALE);

        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxNumericPrecision());
        assertEquals(QueryDataType.MAX_DECIMAL_SCALE, HazelcastTypeSystem.INSTANCE.getMaxNumericScale());

        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxPrecision(DECIMAL));
        assertEquals(QueryDataType.MAX_DECIMAL_SCALE, HazelcastTypeSystem.INSTANCE.getMaxScale(DECIMAL));
    }

    @Test
    public void isObjectTest() {
        assertTrue(isObjectIdentifier(new SqlIdentifier("object", ZERO)));
        assertTrue(isObjectIdentifier(new SqlIdentifier("OBJECT", ZERO)));
        assertFalse(isObjectIdentifier(new SqlIdentifier("foo", ZERO)));
    }

    @Test
    public void withHigherPrecedenceTest() {
        assertPrecedence(type(VARCHAR), type(NULL));
        assertPrecedence(type(BOOLEAN), type(VARCHAR));
        assertPrecedence(type(TINYINT), type(BOOLEAN));
        assertPrecedence(HazelcastIntegerType.create(Byte.SIZE - 1, false), HazelcastIntegerType.create(Byte.SIZE - 2, false));
        assertPrecedence(type(SMALLINT), type(TINYINT));
        assertPrecedence(HazelcastIntegerType.create(Short.SIZE - 1, false), HazelcastIntegerType.create(Short.SIZE - 2, false));
        assertPrecedence(type(INTEGER), type(SMALLINT));
        assertPrecedence(HazelcastIntegerType.create(Integer.SIZE - 1, false), HazelcastIntegerType.create(Integer.SIZE - 2, false));
        assertPrecedence(type(BIGINT), type(INTEGER));
        assertPrecedence(HazelcastIntegerType.create(Long.SIZE - 1, false), HazelcastIntegerType.create(Long.SIZE - 2, false));
        assertPrecedence(type(DECIMAL), type(BIGINT));
        assertPrecedence(type(REAL), type(DECIMAL));
        assertPrecedence(type(DOUBLE), type(REAL));
        assertPrecedence(type(TIME), type(DOUBLE));
        assertPrecedence(type(DATE), type(TIME));
        assertPrecedence(type(TIMESTAMP), type(DATE));
        assertPrecedence(type(TIMESTAMP_WITH_LOCAL_TIME_ZONE), type(TIMESTAMP));
        assertPrecedence(type(ANY), type(TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void deriveSumTypeTest() {
        final HazelcastIntegerType bigint_64 = HazelcastIntegerType.create(64, false);

        assertEquals(type(VARCHAR), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(VARCHAR)));
        assertEquals(type(BOOLEAN), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(BOOLEAN)));
        assertEquals(bigint_64, HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TINYINT)));
        assertEquals(bigint_64, HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(SMALLINT)));
        assertEquals(bigint_64, HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(INTEGER)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(BIGINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DECIMAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(REAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DOUBLE)));
        assertEquals(type(TIME), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIME)));
        assertEquals(type(DATE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DATE)));
        assertEquals(type(TIMESTAMP), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIMESTAMP)));
        assertEquals(
                type(TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIMESTAMP_WITH_LOCAL_TIME_ZONE))
        );
        assertEquals(type(OTHER), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(OTHER)));
    }

    @Test
    public void deriveAvgAggTypeTest() {
        assertEquals(type(VARCHAR), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(VARCHAR)));
        assertEquals(type(BOOLEAN), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(BOOLEAN)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TINYINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(SMALLINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(INTEGER)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(BIGINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DECIMAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(REAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DOUBLE)));
        assertEquals(type(TIME), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIME)));
        assertEquals(type(DATE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DATE)));
        assertEquals(type(TIMESTAMP), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIMESTAMP)));
        assertEquals(
                type(TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIMESTAMP_WITH_LOCAL_TIME_ZONE))
        );
        assertEquals(type(OTHER), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(OTHER)));
    }

    private static void assertPrecedence(RelDataType expected, RelDataType other) {
        RelDataType actual = HazelcastTypeUtils.withHigherPrecedence(expected, other);
        assertSame(expected, actual);
        actual = HazelcastTypeUtils.withHigherPrecedence(other, expected);
        assertSame(expected, actual);
    }

    private static RelDataType type(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }
}
