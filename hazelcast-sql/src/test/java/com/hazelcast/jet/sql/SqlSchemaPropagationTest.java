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

package com.hazelcast.jet.sql;

import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlSchemaPropagationTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SCHEMA_NAME = "schema";

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Before
    public void before() {
        createMapping(MAP_NAME, int.class, int.class);
        instance().getMap(MAP_NAME).put(1, 1);
    }

    @Test
    public void testMember() {
        check(instance());
    }

    @Test
    public void testClient() {
        check(client());
    }

    private void check(HazelcastInstance target) {
        // Set the wrapped optimizer to track optimization requests.
        SqlServiceImpl service = (SqlServiceImpl) instance().getSql();

        // Execute the query from the target without schema.
        SqlStatement statement = new SqlStatement("SELECT __key FROM map");

        List<SqlRow> rows = executeStatement(target, statement);
        assertEquals(1, rows.size());
        assertEquals(1, (int) rows.get(0).getObject(0));

        assertEquals(1, service.getPlanCache().size());

        List<List<String>> originalSearchPaths = Iterables.getOnlyElement(extractSearchPaths());

        // Execute the query from the target with schema.
        statement.setSchema(SCHEMA_NAME);

        rows = executeStatement(target, statement);
        assertEquals(1, rows.size());
        assertEquals(1, (int) rows.get(0).getObject(0));

        assertEquals(2, service.getPlanCache().size());

        List<List<List<String>>> searchPaths = extractSearchPaths();

        List<List<String>> expectedSearchPaths = new ArrayList<>(originalSearchPaths);
        expectedSearchPaths.add(0, asList(CATALOG, SCHEMA_NAME));

        assertThat(searchPaths).containsExactlyInAnyOrder(originalSearchPaths, expectedSearchPaths);
    }

    private static List<SqlRow> executeStatement(HazelcastInstance member, SqlStatement query) {
        List<SqlRow> rows = new ArrayList<>();
        try (SqlResult result = member.getSql().execute(query)) {
            for (SqlRow row : result) {
                rows.add(row);
            }
        }
        return rows;
    }

    private List<List<List<String>>> extractSearchPaths() {
        return ((SqlServiceImpl) instance().getSql())
                .getPlanCache()
                .getPlans()
                .values()
                .stream()
                .map(plan -> plan.getPlanKey().getSearchPaths())
                .collect(toList());
    }
}
