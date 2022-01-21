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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlSeriesGeneratorTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_generateSeriesAscending() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(-5, 5, 5))",
                asList(
                        new Row(-5),
                        new Row(0),
                        new Row(5)
                )
        );
    }

    @Test
    public void test_generateSeriesDescending() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(5, -5, -5))",
                asList(
                        new Row(5),
                        new Row(0),
                        new Row(-5)
                )
        );
    }

    @Test
    public void test_generateSeriesWithDefaultStep() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(0, 2))",
                asList(
                        new Row(0),
                        new Row(1),
                        new Row(2)
                )
        );
    }

    @Test
    public void test_generateSeriesArgumentExpression() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(0, CAST('4' AS INTEGER), 1 + 1))",
                asList(
                        new Row(0),
                        new Row(2),
                        new Row(4)
                )
        );
    }

    @Test
    public void test_generateSeriesNamedArguments() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(step => 1 + 1, stop => 4, \"start\" => 1))",
                asList(
                        new Row(1),
                        new Row(3)
                )
        );
    }

    @Test
    public void test_generateSeriesNamedArgumentsAndDefaultStep() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(stop => 2 + 1, \"start\" => 1))",
                asList(
                        new Row(1),
                        new Row(2),
                        new Row(3)
                )
        );
    }

    @Test
    public void test_generateSeriesNamedArgumentsAndExplicitNull() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(stop => null, \"start\" => null))").iterator().next())
                .hasMessageContaining("GENERATE_SERIES - null argument(s)");
    }

    @Test
    public void test_generateSeriesNamedArgumentsAndExplicitNullStep() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(stop => 2 + 1, \"start\" => 1, step => null))",
                asList(
                        new Row(1),
                        new Row(2),
                        new Row(3)
                )
        );
    }

    @Test
    public void test_generateSeriesFilterAndProject() {
        assertRowsAnyOrder(
                "SELECT v * 2 FROM TABLE(GENERATE_SERIES(0, 5)) WHERE v > 0 AND v < 5",
                asList(
                        new Row(2L),
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateSeriesWithDynamicParameters() {
        assertRowsAnyOrder(
                "SELECT v * ? FROM TABLE(GENERATE_SERIES(0, ?)) WHERE v > ? AND v < 5",
                asList(2, 5, 0),
                asList(
                        new Row(2L),
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateSeriesWithNamedArgumentsAndDynamicParameters() {
        assertRowsAnyOrder(
                "SELECT v * ? FROM TABLE(GENERATE_SERIES(stop => ?, \"start\" => ?)) WHERE v > ? AND v < 5",
                asList(2, 5, 0, 1),
                asList(
                        new Row(4L),
                        new Row(6L),
                        new Row(8L)
                )
        );
    }

    @Test
    public void test_generateSeriesWithDynamicParametersAndArgumentTypeMismatch() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(0, ?))", "1"))
                .hasMessageContaining("Parameter at position 0 must be of INTEGER type, but VARCHAR was found");
    }

    @Test
    public void test_generateSeriesWithNamedArgumentsDynamicParametersAndArgumentTypeMismatch() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(stop => 10, \"start\" => ?))", "1"))
                .hasMessageContaining("Parameter at position 0 must be of INTEGER type, but VARCHAR was found");
    }

    @Test
    public void when_startIsGreaterThanStopAndStepIsPositive_then_returnsEmpty() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(2, 1, 1))",
                emptyList()
        );
    }

    @Test
    public void when_startIsLessThanStopAndStepIsNegative_then_returnsEmpty() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(-2, -1, -1))",
                emptyList()
        );
    }

    @Test
    public void when_stepIsZero_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(0, 5, 0))").iterator().next())
                .hasMessageContaining("GENERATE_SERIES - step cannot be equal to zero");
    }

    @Test
    public void when_coercionIsRequired_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(0, '1'))"))
                .hasMessageContaining("consider adding an explicit CAST");
    }

    @Test
    public void test_nullArgument() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(null, null))").iterator().next())
                .hasMessageContaining("GENERATE_SERIES - null argument(s)");
    }

    @Test
    public void when_notInFromClause_then_throws() {
        createMapping("m", int.class, int.class);
        instance().getMap("m").put(42, 43);
        assertThatThrownBy(() -> sqlService.execute("SELECT GENERATE_SERIES(null, null) FROM m"))
                .hasMessage("Cannot call table function here: 'GENERATE_SERIES'");
    }

    @Test
    public void when_unknownIdentifier_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM TABLE(GENERATE_SERIES(\"start\" => 0, stop => 1, non_existing => 0))"
        )).hasMessageContaining("Unknown argument name 'non_existing'");
    }

    @Test
    public void test_planCache() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(0, 1))",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
        assertThat(planCache(instance()).size()).isEqualTo(1);

        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_SERIES(1, 2))",
                asList(
                        new Row(1),
                        new Row(2)
                )
        );
        assertThat(planCache(instance()).size()).isEqualTo(2);
    }
}
