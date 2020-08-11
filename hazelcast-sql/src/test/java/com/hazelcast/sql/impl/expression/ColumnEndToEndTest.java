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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.util.Collection;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INT;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ColumnEndToEndTest extends ExpressionEndToEndTestBase {

    @Parameter
    public String mapName;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{"serializableRecords"}, {"dataSerializableRecords"}, {"identifiedDataSerializableRecords"},
                {"portableRecords"}});
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    protected Record getRecord() {
        switch (getMapName()) {
            case "serializableRecords":
                return new SerializableRecord();
            case "dataSerializableRecords":
                return new DataSerializableRecord();
            case "identifiedDataSerializableRecords":
                return new IdentifiedDataSerializableRecord();
            case "portableRecords":
                return new PortableRecord();
            default:
                throw new IllegalStateException("unexpected map name");
        }
    }

    @Test
    public void testBoolean() {
        assertRow("booleanTrue", "booleanTrue", BOOLEAN, true);
    }

    @Test
    public void testByte() {
        assertRow("byte1", "byte1", TINYINT, (byte) 1);
    }

    @Test
    public void testShort() {
        assertRow("short1", "short1", SMALLINT, (short) 1);
    }

    @Test
    public void testInt() {
        assertRow("int1", "int1", INT, 1);
    }

    @Test
    public void testLong() {
        assertRow("long1", "long1", BIGINT, 1L);
    }

    @Test
    public void testFloat() {
        assertRow("float1", "float1", REAL, 1.0f);
    }

    @Test
    public void testDouble() {
        assertRow("double1", "double1", DOUBLE, 1.0d);
    }

    @Test
    public void testDecimal() {
        if (getMapName().equals("portableRecords")) {
            assertParsingError("decimal1", "Column 'decimal1' not found in any table");
        } else {
            assertRow("decimal1", "decimal1", DECIMAL, BigDecimal.valueOf(1));
        }
    }

    @Test
    public void testBigInteger() {
        if (getMapName().equals("portableRecords")) {
            assertParsingError("bigInteger1", "Column 'bigInteger1' not found in any table");
        } else {
            assertRow("bigInteger1", "bigInteger1", DECIMAL, BigDecimal.valueOf(1));
        }
    }

    @Test
    public void testString() {
        assertRow("string1", "string1", VARCHAR, "1");
    }

    @Test
    public void testChar() {
        assertRow("char1", "char1", VARCHAR, "1");
    }

    @Test
    public void testObject() {
        if (getMapName().equals("portableRecords")) {
            assertParsingError("object", "Column 'object' not found in any table");
        } else {
            assertRow("object", "object", OBJECT, new SerializableObject());
        }
    }

    @Test
    public void testKey() {
        assertRow("__key", "__key", OBJECT, new RecordKey(0));
        assertRow("id", "id", INT, 0);
    }

    @Test
    public void testValue() {
        assumeTrue(!getMapName().equals("portableRecords"));
        assertRow("this", "this", OBJECT, getRecord());
    }

    @Test
    public void testMissingColumn() {
        assertParsingError("missingColumn", "Column 'missingColumn' not found in any table");
    }

    @Test
    public void testBrokenRecord() {
        IMap<Object, Object> original = getMap();
        String brokenMapName = getMapName() + "Broken";
        IMap<Object, Object> broken = getMap(brokenMapName);

        broken.put(new RecordKey(0), original.get(new RecordKey(0)));
        assertRow("byte1", brokenMapName, "byte1", TINYINT, (byte) 1);

        broken.put(new RecordKey(1), new BrokenRecord());
        assertError("byte1", brokenMapName, SqlErrorCode.DATA_EXCEPTION,
                "Failed to extract map entry value field \"byte1\" because of type mismatch");
    }

}
