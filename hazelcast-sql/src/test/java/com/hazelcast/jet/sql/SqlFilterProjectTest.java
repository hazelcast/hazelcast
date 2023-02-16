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

package com.hazelcast.jet.sql;

import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.schema.function.UserDefinedFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.data.Offset;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlFilterProjectTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_valuesSelect() {
        assertRowsAnyOrder(
                "SELECT * FROM (VALUES ('a'), ('b'))",
                asList(new Row("a"), new Row("b"))
        );
    }

    @BeforeClass
    public static void setUpScripts() {
        // init scripts with tests classloader, job class loader may not see it
        // TODO: will it happen in normal execution?
        // Needed in Java >= 15?
        ScriptEngineManagerContext.getScriptEngineManager();
    }

    @Test
    public void test_valuesSelectScriptUdf() {
        sqlService.execute("create function myfunjs(x varchar) RETURNS varchar\n"
                + "LANGUAGE 'js' \n"
                + "AS `x + '/' + x`");

        assertRowsAnyOrder(
                "SELECT myfunjs(x) as c FROM (VALUES ('a'), ('b')) AS t (x)",
                asList(new Row("a/a"), new Row("b/b"))
        );
    }

    @Test
    public void test_valuesSelectScriptUdfNoParameters() {
        sqlService.execute("create function now() RETURNS bigint\n"
                + "LANGUAGE 'js' \n"
                + "AS `java.lang.System.currentTimeMillis()`");

        assertThat(sqlService.execute("SELECT now()").<Long>scalar())
                .isCloseTo(System.currentTimeMillis(), Offset.offset(10000L));
    }

    @Test
    public void test_valuesSelectScriptUdfNoParametersNoParentheses() {
        assertThatThrownBy(() -> sqlService.execute("create function now RETURNS bigint\n"
                + "LANGUAGE 'js' \n"
                + "AS `java.lang.System.currentTimeMillis()`"))
                .hasMessageContaining("Encountered \"RETURNS\"");
    }

    @Test
    public void test_dropUdf() {
        sqlService.execute("create function myfunjs(x varchar) RETURNS varchar\n"
                + "LANGUAGE 'js' \n"
                + "AS `x + '/' + x`");
        sqlService.execute("drop function myfunjs");
        assertThatThrownBy(() -> sqlService.execute("SELECT myfunjs(x) as c FROM (VALUES ('a'), ('b')) AS t (x)"))
                .hasMessageContaining("Function 'myfunjs' does not exist");
    }

    @Test
    public void test_valuesSelectScriptUdf_directCreate() {
        UserDefinedFunction function = new UserDefinedFunction("myfunjs", "js",
                QueryDataType.VARCHAR,
                singletonList("x"), singletonList(QueryDataType.VARCHAR),
                "x + '/' + x"
        );
        createFunction(function);

        assertRowsAnyOrder(
                "SELECT myfunjs(x) as c FROM (VALUES ('a'), ('b')) AS t (x)",
                asList(new Row("a/a"), new Row("b/b"))
        );
    }

    @Test
    public void test_valuesSelectScriptUdfNested() {
        UserDefinedFunction function = new UserDefinedFunction("factorial", "js",
                QueryDataType.INT,
                singletonList("x"), singletonList(QueryDataType.INT),
                "function factorial_impl(v) {\n" +
                        "    var result = 1;\n" +
                        "    for(var i=2;i<=v;i++)\n" +
                        "        result *= i;\n" +
                        "    return result;\n" +
                        "}\n" +
                        "factorial_impl(x);"
        );
        createFunction(function);

        assertRowsAnyOrder(
                "SELECT factorial(v) as c FROM TABLE (generate_series(0,5))",
                asList(new Row(1), new Row(1),
                        new Row(2), new Row(6),
                        new Row(24), new Row(120))
        );

        assertRowsAnyOrder(
                "SELECT avg(factorial(v)) as c FROM TABLE (generate_series(1,5))",
                asList(new Row(BigDecimal.valueOf(30.6)))
        );
    }

    @Test
    public void test_valuesSelectScriptUdfGroovy() {
        sqlService.execute("create function is_prime(n BIGINT) RETURNS varchar\n"
                + "LANGUAGE 'groovy' \n"
                + "AS `def isPrime(i) { i <=2 || (2..Math.sqrt(i)).every { i % it != 0 } }\n" +
                "\n" +
                "isPrime(n) ? \"prime\" : \"composite\"`");

        assertRowsAnyOrder(
                "SELECT v, is_prime(v) FROM TABLE (generate_series(1,6))",
                asList(new Row(1, "prime"), new Row(2, "prime"),
                        new Row(3, "prime"), new Row(4, "composite"),
                        new Row(5, "prime"), new Row(6, "composite"))
        );
    }

    @Test
    public void test_valuesSelectScriptUdfPython() {
        sqlService.execute("create function getval(num DOUBLE) RETURNS VARCHAR\n"
                + "LANGUAGE 'python' \n"
                + "AS `num_sqrt = num ** 0.5\n"
                + "result = 'Square root of %0.3f is %0.3f'%(num ,num_sqrt)\n`");

        assertRowsAnyOrder(
                "SELECT v, getval(v) FROM TABLE (generate_series(1,6))",
                asList(new Row(1, "Square root of 1.000 is 1.000"),
                        new Row(2, "Square root of 2.000 is 1.414"),
                        new Row(3, "Square root of 3.000 is 1.732"),
                        new Row(4, "Square root of 4.000 is 2.000"),
                        new Row(5, "Square root of 5.000 is 2.236"),
                        new Row(6, "Square root of 6.000 is 2.449"))
        );
    }

    @Test
    public void test_valuesSelectScriptUdf_getDDL() {
        sqlService.execute("create function myfunjs(x varchar) RETURNS varchar\n"
                + "LANGUAGE 'js' \n"
                + "AS `x + '/' + x`");

        assertThat(sqlService.execute("SELECT get_ddl('relation', 'myfunjs')").<String>scalar())
                .isEqualTo("CREATE FUNCTION \"myfunjs\" (x VARCHAR) RETURNS VARCHAR\n" +
                                "LANGUAGE 'js'\n" +
                                "AS `x + '/' + x`");
    }

    @Test
    public void test_valuesSelectScriptUdfWithSql() {
        UserDefinedFunction function = new UserDefinedFunction("myfunjs", "js",
                QueryDataType.VARCHAR,
                singletonList("x"), singletonList(QueryDataType.VARCHAR),
                "sql.execute('select UPPER(?) || ?', x, x).scalar()"
        );
        createFunction(function);

        assertRowsAnyOrder(
                "SELECT myfunjs(x) as c FROM (VALUES ('a'), ('b')) AS t (x)",
                asList(new Row("Aa"), new Row("Bb"))
        );
    }

    private static void createFunction(UserDefinedFunction function) {
        NodeEngineImpl nodeEngine = Accessors.getNodeEngineImpl(instance());
        TablesStorage tablesStorage = new TablesStorage(nodeEngine);
        SqlConnectorCache connectorCache = new SqlConnectorCache(nodeEngine);
        TableResolverImpl tableResolver = new TableResolverImpl(nodeEngine, tablesStorage, connectorCache);
        tableResolver.createFunction(
                function,
                true, false);
    }

    @Test
    public void test_valuesSelectExpression() {
        assertRowsAnyOrder(
                "SELECT * FROM (VALUES (1), (1 + 2), (CAST ('5' AS TINYINT)))",
                asList(new Row((byte) 1), new Row((byte) 3), new Row((byte) 5))
        );
    }

    @Test
    public void test_valuesSelectFilter() {
        assertRowsAnyOrder(
                "SELECT a - b FROM (VALUES (1, 2), (3, 5), (7, 11)) AS t (a, b) WHERE a > 1",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void test_valuesSelectFilterExpression() {
        assertRowsAnyOrder(
                "SELECT a - b FROM ("
                        + "VALUES (1, 2), (3, 5), (7, 11)"
                        + ") AS t (a, b) "
                        + "WHERE a + b + 0 + CAST('1' AS TINYINT) > 4",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void test_valuesSelectExpressionFilterExpression() {
        assertRowsAnyOrder(
                "SELECT a - b FROM ("
                        + "VALUES (1, 1 + 1), (3, 5), (CAST('7' AS TINYINT), 11)"
                        + ") AS t (a, b) "
                        + "WHERE a + b + 0 + CAST('1' AS TINYINT) > 4",
                asList(
                        new Row((short) -2),
                        new Row((short) -4)
                )
        );
    }

    @Test
    public void test_valuesSelectDynamicParameters() {
        assertRowsAnyOrder(
                "SELECT ? - b FROM ("
                        + "VALUES (1, ? + 1), (3, 5), (CAST(? AS TINYINT), 11)"
                        + ") AS t (a, b) "
                        + "WHERE a + b + ? + CAST('1' AS TINYINT) > ?",
                asList(42, 1, "7", 0, 4),
                asList(
                        new Row(37L),
                        new Row(31L)
                )
        );
    }

    @Test
    public void test_valuesInsert() {
        createMapping("m", Integer.class, Integer.class);

        assertMapEventually(
                "m",
                "SINK INTO m(__key, this) VALUES (1, 1), (2, 2)",
                createMap(1, 1, 2, 2)
        );
    }

    @Test
    public void test_valuesInsertExpression() {
        createMapping("m", Integer.class, Integer.class);

        assertMapEventually(
                "m",
                "SINK INTO m(__key, this) VALUES "
                        + "(CAST(1 AS INTEGER), CAST(1 + 0 AS INTEGER))"
                        + ", (CAST(2 AS INTEGER), CAST(2 AS INTEGER))",
                createMap(1, 1, 2, 2)
        );
    }

    @Test
    public void test_valuesInsertDynamicParameter() {
        createMapping("m", Integer.class, String.class);

        assertMapEventually(
                "m",
                "SINK INTO m(__key, this) VALUES (? + 1, ?), (?, UPPER(?))",
                asList(0, "a", 2, "b"),
                createMap(1, "a", 2, "B")
        );
    }

    @Test
    public void test_projectWithoutInputReferences() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT '喷气式飞机' FROM t",
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")
                )
        );
    }

    @Test
    public void test_starProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM t",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t)",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t WHERE 0 = 0) WHERE 1 = 1",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectFilterExpressionProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t WHERE 0 = 0) WHERE 2 - 1 = 1",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_project() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT v, v FROM t",
                asList(
                        new Row(0, 0),
                        new Row(1, 1)
                )
        );
    }

    @Test
    public void test_projectProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t)",
                asList(
                        new Row(0, 0),
                        new Row(1, 1)
                )
        );
    }

    @Test
    public void test_projectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT v + 1, v * v FROM t",
                asList(
                        new Row(1L, 0L),
                        new Row(2L, 1L)
                )
        );
    }

    @Test
    public void test_projectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v, v FROM t WHERE v = 1 OR v = 2",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v, v FROM t WHERE v + v > 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v + 1, v + v FROM t WHERE v >= 1",
                asList(
                        new Row(2L, 2L),
                        new Row(3L, 4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v + 1, v + v FROM t WHERE v + v > 1",
                asList(
                        new Row(2L, 2L),
                        new Row(3L, 4L)
                )
        );
    }

    @Test
    public void test_projectProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t)",
                asList(
                        new Row(0L, 0),
                        new Row(2L, 1)
                )
        );
    }

    @Test
    public void test_projectExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t)",
                asList(
                        new Row(0L),
                        new Row(2L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t)",
                asList(
                        new Row(0L),
                        new Row(3L)
                )
        );
    }

    @Test
    public void test_projectProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectProjectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectFilterProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t) WHERE f2 >= 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 >= 1",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 + f2 > 2",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t) WHERE f2 >= 1",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 >= 1",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1) WHERE f2 < 3",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1) WHERE f2 < 6",
                asList(
                        new Row(1),
                        new Row(2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1) WHERE f2 < 3",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1) WHERE f2 < 6",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1) WHERE f1 + f2 < 9",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_explicitTable() {
        TestBatchSqlConnector.create(sqlService, "t", 2);
        assertRowsAnyOrder("table t",
                rows(1, 0, 1));
        assertRowsAnyOrder("(table t)",
                rows(1, 0, 1));
        assertRowsAnyOrder("select * from (table t) t",
                rows(1, 0, 1));
    }

    @Test
    public void test_queryMetadata() {
        TestAllTypesSqlConnector.create(sqlService, "t");

        SqlResult result = sqlService.execute("SELECT * FROM t");

        assertThat(result.updateCount()).isEqualTo(-1);
        assertThat(result.getRowMetadata().getColumnCount()).isEqualTo(15);
        assertThat(result.getRowMetadata().getColumn(0).getName()).isEqualTo("string");
        assertThat(result.getRowMetadata().getColumn(0).getType()).isEqualTo(SqlColumnType.VARCHAR);
        assertThat(result.getRowMetadata().getColumn(1).getName()).isEqualTo("boolean");
        assertThat(result.getRowMetadata().getColumn(1).getType()).isEqualTo(SqlColumnType.BOOLEAN);
        assertThat(result.getRowMetadata().getColumn(2).getName()).isEqualTo("byte");
        assertThat(result.getRowMetadata().getColumn(2).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat(result.getRowMetadata().getColumn(3).getName()).isEqualTo("short");
        assertThat(result.getRowMetadata().getColumn(3).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat(result.getRowMetadata().getColumn(4).getName()).isEqualTo("int");
        assertThat(result.getRowMetadata().getColumn(4).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat(result.getRowMetadata().getColumn(5).getName()).isEqualTo("long");
        assertThat(result.getRowMetadata().getColumn(5).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat(result.getRowMetadata().getColumn(6).getName()).isEqualTo("float");
        assertThat(result.getRowMetadata().getColumn(6).getType()).isEqualTo(SqlColumnType.REAL);
        assertThat(result.getRowMetadata().getColumn(7).getName()).isEqualTo("double");
        assertThat(result.getRowMetadata().getColumn(7).getType()).isEqualTo(SqlColumnType.DOUBLE);
        assertThat(result.getRowMetadata().getColumn(8).getName()).isEqualTo("decimal");
        assertThat(result.getRowMetadata().getColumn(8).getType()).isEqualTo(SqlColumnType.DECIMAL);
        assertThat(result.getRowMetadata().getColumn(9).getName()).isEqualTo("time");
        assertThat(result.getRowMetadata().getColumn(9).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat(result.getRowMetadata().getColumn(10).getName()).isEqualTo("date");
        assertThat(result.getRowMetadata().getColumn(10).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat(result.getRowMetadata().getColumn(11).getName()).isEqualTo("timestamp");
        assertThat(result.getRowMetadata().getColumn(11).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat(result.getRowMetadata().getColumn(12).getName()).isEqualTo("timestampTz");
        assertThat(result.getRowMetadata().getColumn(12).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(result.getRowMetadata().getColumn(13).getName()).isEqualTo("map");
        assertThat(result.getRowMetadata().getColumn(13).getType()).isEqualTo(SqlColumnType.OBJECT);
        assertThat(result.getRowMetadata().getColumn(14).getName()).isEqualTo("object");
        assertThat(result.getRowMetadata().getColumn(14).getType()).isEqualTo(SqlColumnType.OBJECT);
    }

    @Test
    public void test_sinkMetadata() {
        createMapping("m", Integer.class, Integer.class);

        SqlResult result = sqlService.execute("SINK INTO m(__key, this) VALUES (1, 1), (2, 2)");

        assertThat(result.updateCount()).isEqualTo(0);
    }

    @Test
    public void test_dynamicParameterMetadata() {
        TestBatchSqlConnector.create(sqlService, "t", 1);

        SqlResult result = sqlService.execute("SELECT CAST(? AS VARCHAR) FROM t", 1);
        assertThat(result.getRowMetadata().getColumnCount()).isEqualTo(1);
        assertThat(result.getRowMetadata().getColumn(0).getType()).isEqualTo(SqlColumnType.VARCHAR);
    }

    @Test
    public void test_dynamicParameterCountMismatch() {
        TestBatchSqlConnector.create(sqlService, "t", 1);

        assertThatThrownBy(() -> sqlService.execute("SELECT CAST(? AS VARCHAR) FROM t", 1, 2))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Unexpected parameter count: expected 1, got 2");
    }

    @Test // for https://github.com/hazelcast/hazelcast/issues/21640
    public void test_selectByKey_deoptToFullScan() {
        String mapOneName = "mapOne";
        createMapping(mapOneName, int.class, String.class);

        String mapTwoName = "mapTwo";
        createMapping(mapTwoName, int.class, String.class);

        instance().getMap(mapOneName).put(1, "value-1");
        instance().getMap(mapOneName).put(2, "value-2");
        instance().getMap(mapOneName).put(3, "value-3");

        instance().getMap(mapTwoName).put(1, "value-1");
        instance().getMap(mapTwoName).put(2, "value-2");
        instance().getMap(mapTwoName).put(3, "value-3");

        assertRowsAnyOrder(
                "SELECT o.this as mId FROM mapOne AS o" +
                        " JOIN mapTwo AS a ON o.__key = a.__key" +
                        " WHERE a.__key IN (1)",
                singletonList(new Row("value-1"))
        );
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/19983
    // Checks the case when select-by-key optimization is used, but the __key (the 0-th) field isn't selected.
    public void test_selectByKey_keyNotSelected() {
        IMap<Long, Person> map = instance().getMap("test");
        map.put(1L, new Person(10, "foo"));

        createMapping("test", Long.class, Person.class);
        assertRowsAnyOrder("select name from test where __key = 1", singletonList(new Row("foo")));
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/21954
    public void test_filterSpecificValueOrNull() {
        String mapName = "test";
        createMapping(mapName, Integer.class, Person.class);

        IMap<Object, Object> map = instance().getMap(mapName);
        map.put(42, new Person(42, null));
        map.put(43, new Person(43, "foo"));

        assertRowsAnyOrder("select name from " + mapName + " where name is null or name='foo'", rows(1, null, "foo"));
        assertRowsAnyOrder("select __key from " + mapName + " where name is null or name='foo'", rows(1, 42, 43));
    }
}
