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

package com.hazelcast.jet.sql;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.sql.impl.SqlTestSupport.nodeEngine;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(SqlTestSupport.class);

    @After
    public void tearDown() {
        for (JetInstance instance : instances()) {
            PlanCache planCache = planCache(instance);
            SUPPORT_LOGGER.info("Removing " + planCache.size() + " cached plans in SqlTestSupport.@After");
            planCache.clear();
        }
    }

    /**
     * Execute a query and assert that it eventually contains the expected entries.
     *
     * @param mapName  The IMap name
     * @param sql      The query
     * @param expected Expected IMap contents after executing the query
     */
    public static <K, V> void assertMapEventually(String mapName, String sql, Map<K, V> expected) {
        assertMapEventually(mapName, sql, emptyList(), expected);
    }

    /**
     * Execute a query and assert that it eventually contains the expected entries.
     *
     * @param mapName   The IMap name
     * @param sql       The query
     * @param arguments The query arguments
     * @param expected  Expected IMap contents after executing the query
     */
    public static <K, V> void assertMapEventually(String mapName, String sql, List<Object> arguments, Map<K, V> expected) {
        SqlStatement statement = new SqlStatement(sql);
        arguments.forEach(statement::addParameter);

        instance().getSql().execute(statement);

        Map<K, V> map = instance().getMap(mapName);
        assertTrueEventually(() ->
                assertThat(new HashMap<>(map)).containsExactlyInAnyOrderEntriesOf(expected), 10);
    }

    public static void assertRowsEventuallyInAnyOrder(String sql, Collection<Row> expectedRows) {
        assertRowsEventuallyInAnyOrder(sql, emptyList(), expectedRows);
    }

    /**
     * Execute a query and wait for the results to contain all the {@code
     * expectedRows}. If there are more rows in the result, they are ignored.
     * Suitable for streaming queries that don't terminate.
     *
     * @param sql          The query
     * @param arguments   The query arguments
     * @param expectedRows Expected rows
     */
    public static void assertRowsEventuallyInAnyOrder(String sql, List<Object> arguments, Collection<Row> expectedRows) {
        SqlService sqlService = instance().getSql();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Deque<Row> rows = new ArrayDeque<>();

        Thread thread = new Thread(() -> {
            SqlStatement statement = new SqlStatement(sql);
            arguments.forEach(statement::addParameter);

            try (SqlResult result = sqlService.execute(statement)) {
                Iterator<SqlRow> iterator = result.iterator();
                for (int i = 0; i < expectedRows.size() && iterator.hasNext(); i++) {
                    rows.add(new Row(iterator.next()));
                }
                future.complete(null);
            } catch (Throwable e) {
                e.printStackTrace();
                future.completeExceptionally(e);
            }
        });

        thread.start();

        try {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                thread.interrupt();
                thread.join();
            }
        } catch (Exception e) {
            throw sneakyThrow(e);
        }

        List<Row> actualRows = new ArrayList<>(rows);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in any order.
     *
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) {
        assertRowsAnyOrder(sql, emptyList(), expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in any order.
     *
     * @param sql          The query
     * @param arguments    The query arguments
     * @param expectedRows Expected rows
     */
    public static void assertRowsAnyOrder(String sql, List<Object> arguments, Collection<Row> expectedRows) {
        SqlStatement statement = new SqlStatement(sql);
        arguments.forEach(statement::addParameter);

        SqlService sqlService = instance().getSql();
        List<Row> actualRows = new ArrayList<>();
        sqlService.execute(statement).iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in the given order.
     *
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertRowsOrdered(String sql, List<Row> expectedRows) {
        SqlService sqlService = instance().getSql();
        List<Row> actualRows = new ArrayList<>();
        sqlService.execute(sql).iterator().forEachRemaining(r -> actualRows.add(new Row(r)));
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    /**
     * Create DDL for an IMap with the given {@code name}, that uses
     * java serialization for both key and value with the given classes.
     */
    public static String javaSerializableMapDdl(String name, Class<?> keyClass, Class<?> valueClass) {
        return "CREATE MAPPING " + name + " TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
               + "OPTIONS (\n"
               + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "',\n"
               + '\'' + OPTION_KEY_CLASS + "'='" + keyClass.getName() + "',\n"
               + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "',\n"
               + '\'' + OPTION_VALUE_CLASS + "'='" + valueClass.getName() + "'\n"
               + ")";
    }

    public static String randomName() {
        // Prefix the UUID with some letters and remove dashes so that it doesn't start with
        // a number and is a valid SQL identifier without quoting.
        return "o_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    /**
     * Compares two lists. The lists are expected to contain elements of type
     * Object[]. Useful for {@link TestSupport#outputChecker(BiPredicate)}.
     */
    public static boolean compareRowLists(List<?> expected, List<?> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Object[] expectedItem = (Object[]) expected.get(i);
            Object[] actualItem = (Object[]) actual.get(i);
            if (!Arrays.equals(expectedItem, actualItem)) {
                return false;
            }
        }

        return true;
    }

    public static String hadoopNonExistingPath() {
        return System.getProperty("os.name").toLowerCase().contains("windows")
                ? "c:\\non\\existing\\path"
                : "/non/existing/path";
    }

    public static PlanCache planCache(JetInstance instance) {
        return nodeEngine(instance.getHazelcastInstance()).getSqlService().getPlanCache();
    }

    /**
     * A class passed to utility methods in this class. We don't use SqlRow
     * directly because:
     * - SqlRow doesn't implement `equals`
     * - It's not easy to create SqlRow instance
     */
    public static final class Row {

        private final Object[] values;

        private Row(SqlRow row) {
            values = new Object[row.getMetadata().getColumnCount()];
            for (int i = 0; i < values.length; i++) {
                values[i] = row.getObject(i);
            }
        }

        public Row(Object... values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "Row{" + Arrays.toString(values) + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
