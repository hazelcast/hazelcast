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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPrimitiveTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_sinkSelect() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        String from = randomName();
        TestBatchSqlConnector.create(sqlService, from, 4);

        assertMapEventually(
                name,
                "SINK INTO " + name + " SELECT v, 'value-' || v FROM " + from,
                createMap(0, "value-0", 1, "value-1", 2, "value-2", 3, "value-3")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name + " WHERE __key > 0 AND __key < 3",
                asList(
                        new Row(1, "value-1"),
                        new Row(2, "value-2")
                )
        );
    }

    @Test
    public void test_sinkValues() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertMapEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_sinkWithProject() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertMapEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES ('2', CAST(0 + 1 AS INT))",
                createMap(1, "2")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_sinkWithDynamicParameters() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertMapEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES (?, CAST(0 + ? AS INT))",
                asList("2", 1),
                createMap(1, "2")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_selectWithDynamicParameters() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        sqlService.execute("SINK INTO " + name + " VALUES (1, '1'), (2, '2')");

        assertRowsAnyOrder(
                "SELECT __key + ?, ABS(__key + ?), this FROM " + name + " WHERE __key + ? >= ?",
                asList(2, -10, 2, 4),
                singletonList(new Row(4L, 8L, "2"))
        );
    }

    @Test
    public void test_renameKey() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "id INT EXTERNAL NAME __key"
                + ", this VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        )).hasMessageContaining("Cannot rename field: '__key'");

        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT EXTERNAL NAME renamed"
                + ", this VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        )).hasMessageContaining("Cannot rename field: '__key'");
    }

    @Test
    public void test_renameThis() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        )).hasMessageContaining("Cannot rename field: 'this'");

        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT"
                + ", this VARCHAR EXTERNAL NAME renamed"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        )).hasMessageContaining("Cannot rename field: 'this'");
    }

    @Test
    public void test_objectAndMappingNameDifferent() {
        String mapName = randomName();
        String tableName = randomName();

        sqlService.execute("CREATE MAPPING " + tableName + " EXTERNAL NAME " + mapName + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + String.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")"
        );

        IMap<String, String> map = instance().getMap(mapName);
        map.put("k1", "v1");
        map.put("k2", "v2");

        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                asList(
                        new Row("k1", "v1"),
                        new Row("k2", "v2")
                )
        );
    }

    @Test
    public void test_explicitKeyAndThis() {
        String mapName = randomName();

        sqlService.execute("CREATE MAPPING " + mapName + '('
                + "__key INT,"
                + "this VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + mapName + " VALUES(42, 'foo')");

        assertRowsAnyOrder("SELECT * FROM " + mapName,
                singletonList(new Row(42, "foo")));
    }

    @Test
    public void test_explicitKeyAndThisWithExternalNames() {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + '('
                + "__key INT EXTERNAL NAME __key"
                + ", this VARCHAR EXTERNAL NAME this"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + mapName + " VALUES(42, 'foo')");

        assertRowsAnyOrder("SELECT * FROM " + mapName,
                singletonList(new Row(42, "foo")));
    }

    @Test
    public void test_noKeyFormat() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")"
        )).hasMessage("Missing 'keyFormat' option");
    }

    @Test
    public void test_noValueFormat() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ")"
        )).hasMessage("Missing 'valueFormat' option");
    }

    @Test
    public void when_keyColumnIsNotDeclared_then_itIsHidden() {
        String name = randomName();

        sqlService.execute("CREATE MAPPING " + name + '('
                + "this VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        );

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + name + " VALUES(1, 'Alice')"))
                .hasMessageContaining("Number of INSERT target columns (1) does not equal number of source items (2)");

        assertMapEventually(
                name,
                "SINK INTO " + name + "(__key, this) VALUES (2, 'Bob')",
                createMap(2, "Bob")
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Bob"))
        );
        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(2, "Bob"))
        );
    }

    @Test
    public void when_valueColumnIsNotDeclared_then_itIsHidden() {
        String name = randomName();

        sqlService.execute("CREATE MAPPING " + name + '('
                + "__key INT EXTERNAL NAME __key"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ")"
        );

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + name + " VALUES(1, 'Alice')"))
                .hasMessageContaining("Number of INSERT target columns (1) does not equal number of source items (2)");

        assertMapEventually(
                name,
                "SINK INTO " + name + "(__key, this) VALUES (2, 'Bob')",
                createMap(2, "Bob")
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(2))
        );
        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(2, "Bob"))
        );
    }

    @Test
    public void when_insert() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertMapEventually(
                name,
                "INSERT INTO " + name + " (this, __key) VALUES ('1', 1)",
                createMap(1, "1")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "1"))
        );
    }

    @Test
    public void when_insertMultipleEntries() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertMapEventually(
                name,
                "INSERT INTO " + name + " (this, __key) VALUES ('1', 1), ('2', 2)",
                createMap(1, "1", 2, "2")
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "1"),
                        new Row(2, "2")
                )
        );
    }

    @Test
    public void when_insertNoEntries() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        sqlService.execute("INSERT INTO " + name + " SELECT * FROM (VALUES (1, '1')) AS t(a, b) WHERE a = 0");
        assertThat(instance().getMap(name).entrySet()).isEmpty();
    }

    @Test
    public void when_insertAndKeyAlreadyExists_then_fail() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);
        sqlService.execute("INSERT INTO " + name + " VALUES (1, '1')");

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " VALUES (1, '2')"))
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_insertMultipleEntriesAndKeyAlreadyExists_then_fail() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);
        sqlService.execute("INSERT INTO " + name + " VALUES (1, '1')");

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " VALUES (1, '2'), (2, '2')"))
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_insertDuplicateKey_then_fail() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " VALUES (1, '1'), (1, '2')"))
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_typeMismatch_then_fail() {
        String name = randomName();
        instance().getMap(name).put(0, 0);
        createMapping(name, String.class, String.class);

        assertThatThrownBy(() -> sqlService.execute("SELECT __key FROM " + name).iterator().forEachRemaining(row -> {
        }))
                .hasMessageContaining("Failed to extract map entry key because of type mismatch " +
                        "[expectedClass=java.lang.String, actualClass=java.lang.Integer]");
    }

    @Test
    public void test_multipleFieldsForPrimitive_key() {
        test_multipleFieldsForPrimitive("__key");
    }

    @Test
    public void test_multipleFieldsForPrimitive_value() {
        test_multipleFieldsForPrimitive("this");
    }

    private void test_multipleFieldsForPrimitive(String fieldName) {
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING map ("
                        + fieldName + " INT"
                        + ", field INT EXTERNAL NAME \"" + fieldName + ".field\""
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME
                        + " OPTIONS ("
                        + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Integer.class.getName() + '\''
                        + ", '" + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                        + ")"))
                .hasMessage(
                        "The field '" + fieldName + "' is of type INTEGER, you can't map '" + fieldName + ".field' too");
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_varchar() {
        test_simplePrimitiveTypeSyntax("varchar", "foo");
        test_simplePrimitiveTypeSyntax("character varying", "foo");
        test_simplePrimitiveTypeSyntax("char varying", "foo");
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_boolean() {
        test_simplePrimitiveTypeSyntax("boolean", true);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_tinyint() {
        test_simplePrimitiveTypeSyntax("tinyint", (byte) 42);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_smallint() {
        test_simplePrimitiveTypeSyntax("smallint", (short) 42);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_int() {
        test_simplePrimitiveTypeSyntax("int", 42);
        test_simplePrimitiveTypeSyntax("integer", 42);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_bigint() {
        test_simplePrimitiveTypeSyntax("bigint", 42L);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_decimal() {
        test_simplePrimitiveTypeSyntax("decimal", BigDecimal.valueOf(42));
        test_simplePrimitiveTypeSyntax("dec", BigDecimal.valueOf(42));
        test_simplePrimitiveTypeSyntax("numeric", BigDecimal.valueOf(42));
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_real() {
        test_simplePrimitiveTypeSyntax("real", 42f);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_double() {
        test_simplePrimitiveTypeSyntax("double", 42d);
        test_simplePrimitiveTypeSyntax("double precision", 42d);
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_time() {
        test_simplePrimitiveTypeSyntax("time", LocalTime.of(10, 42));
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_date() {
        test_simplePrimitiveTypeSyntax("date", LocalDate.of(1942, 4, 2));
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_timestamp() {
        test_simplePrimitiveTypeSyntax("timestamp", LocalDateTime.of(1942, 4, 2, 10, 42));
    }

    @Test
    public void test_simplePrimitiveTypeSyntax_timestampWithTz() {
        test_simplePrimitiveTypeSyntax("timestamp with time zone",
                OffsetDateTime.of(1042, 4, 2, 10, 42, 0, 0, ZoneOffset.ofHours(4)));
    }

    private void test_simplePrimitiveTypeSyntax(String format, Object testValue) {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + " TYPE IMap " +
                "OPTIONS ('keyFormat'='" + format + "'," +
                "'valueFormat'='" + format + "')");

        instance().getMap(mapName).put(testValue, testValue);

        assertRowsAnyOrder("SELECT * FROM " + mapName, singletonList(new Row(testValue, testValue)));
    }

    @Test
    // reproducer for https://github.com/hazelcast/hazelcast/issues/21252
    public void test_selectByKey_nonExistentKey() {
        instance().getSql().execute("CREATE MAPPING m "
                + " TYPE IMap  OPTIONS ("
                + " 'keyFormat' = 'int',"
                + " 'valueFormat' = 'int')");

        assertRowsAnyOrder(instance(), "SELECT * FROM m WHERE __key = 1", emptyList());
    }
}
