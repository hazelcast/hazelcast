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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SqlStreamGeneratorTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_generateStream() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(100))",
                asList(
                        new Row(0L),
                        new Row(1L),
                        new Row(2L)
                )
        );
    }

    @Test
    public void test_generateStreamArgumentExpression() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(CAST(CAST('50' AS INTEGER) + 50 AS INT)))",
                asList(
                        new Row(0L),
                        new Row(1L),
                        new Row(2L)
                )
        );
    }

    @Test
    public void test_generateStreamNamedArguments() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(rate => 50 + 50))",
                asList(
                        new Row(0L),
                        new Row(1L),
                        new Row(2L)
                )
        );
    }

    @Test
    public void test_generateStreamNamedArgumentsAndExplicitNull() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(rate => null))").iterator().next())
                .hasMessageContaining("GENERATE_STREAM - rate cannot be null");
    }

    @Test
    public void test_generateStreamFilterAndProject() {
        assertRowsEventuallyInAnyOrder(
                "SELECT v * 2 FROM TABLE(GENERATE_STREAM(100)) WHERE v > 0 AND v < 5",
                asList(
                        new Row(2L),
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateStreamWithDynamicParameters() {
        assertRowsEventuallyInAnyOrder(
                "SELECT v * ? FROM TABLE(GENERATE_STREAM(?)) WHERE v > 1 - ? AND v < 5",
                asList(2, 100, 1),
                asList(
                        new Row(2L),
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateStreamWithNamedArgumentsAndDynamicParameters() {
        assertRowsEventuallyInAnyOrder(
                "SELECT v * ? FROM TABLE(GENERATE_STREAM(rate => ?)) WHERE v > 1 - ? AND v < 5",
                asList(2, 100, 1),
                asList(
                        new Row(2L),
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateStreamWithDynamicParametersAndArgumentTypeMismatch() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(?))", "1"))
                .hasMessageContaining("Parameter at position 0 must be of INTEGER type, but VARCHAR was found");
    }

    @Test
    public void test_generateStreamWithNamedArgumentsDynamicParametersAndArgumentTypeMismatch() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(rate => ?))", "1"))
                .hasMessageContaining("Parameter at position 0 must be of INTEGER type, but VARCHAR was found");
    }

    @Test
    public void test_generateEmptyStream() {
        SqlResult result = sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(0))");
        Future<Boolean> future = spawn(() -> result.iterator().hasNext());
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
        result.close();
        assertTrueEventually(() -> assertTrue(future.isDone()));
    }

    @Test
    public void when_rateIsNegative_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(-1))").iterator().next())
                .hasMessageContaining("GENERATE_STREAM - rate cannot be less than zero");
    }

    @Test
    public void when_coercionIsRequired_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM('100'))"))
                .hasMessageContaining("consider adding an explicit CAST");
    }

    @Test
    public void test_nullArgument() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(null))").iterator().next())
                .hasMessageContaining("GENERATE_STREAM - rate cannot be null");
    }

    @Test
    public void when_notInFromClause_then_throws() {
        IMap<Integer, Integer> map = instance().getMap("m");
        map.put(42, 43);
        assertThatThrownBy(() -> sqlService.execute("SELECT GENERATE_STREAM(null) FROM m"))
                .hasMessage("unexpected SQL type: ROW");
    }

    @Test
    public void when_unknownIdentifier_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(non_existing => 0))"))
                .hasMessageContaining("Unknown argument name 'non_existing'");
    }

    @Test
    public void test_planCache() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(100))",
                asList(
                        new Row(0L),
                        new Row(1L)
                )
        );
        assertThat(planCache(instance()).size()).isEqualTo(1);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(200))",
                asList(
                        new Row(0L),
                        new Row(1L)
                )
        );
        assertThat(planCache(instance()).size()).isEqualTo(2);
    }
}
