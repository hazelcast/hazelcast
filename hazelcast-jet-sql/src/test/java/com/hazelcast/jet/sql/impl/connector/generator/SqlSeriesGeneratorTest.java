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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
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
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_SERIES(0, 5, 0))"))
                .hasMessageContaining("step cannot equal zero");
    }
}
