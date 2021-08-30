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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.test.Accessors;
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
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(SqlTestSupport.class);

    @After
    public void tearDown() {
        //noinspection ConstantConditions
        if (instances() == null) {
            return;
        }

        for (HazelcastInstance instance : instances()) {
            PlanCache planCache = planCache(instance);
            SUPPORT_LOGGER.info("Removing " + planCache.size() + " cached plans in SqlTestSupport.@After");
            planCache.clear();
        }
    }

    /**
     * Execute a query and assert that it eventually returns the expected entries.
     *
     * @param mapName  The IMap name
     * @param sql      The query
     * @param expected Expected IMap contents after executing the query
     */
    public static <K, V> void assertMapEventually(String mapName, String sql, Map<K, V> expected) {
        assertMapEventually(mapName, sql, emptyList(), expected);
    }

    /**
     * Execute a query and assert that it eventually returns the expected entries.
     *
     * @param mapName   The IMap name
     * @param sql       The query
     * @param arguments The query arguments
     * @param expected  Expected IMap contents after executing the query
     */
    public static <K, V> void assertMapEventually(String mapName, String sql, List<Object> arguments, Map<K, V> expected) {
        SqlStatement statement = new SqlStatement(sql);
        statement.setParameters(arguments);

        //noinspection EmptyTryBlock
        try (@SuppressWarnings("unused") SqlResult result = instance().getSql().execute(statement)) {
        }

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
     * @param arguments    The query arguments
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
        try (SqlResult result = sqlService.execute(statement)) {
            result.iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        }
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
        try (SqlResult result = sqlService.execute(sql)) {
            result.iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        }
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for both key and value with the given classes.
     */
    public static void createMapping(String name, Class<?> keyClass, Class<?> valueClass) {
        createMapping(instance(), name, keyClass, valueClass);
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for both key and value with the given classes.
     */
    public static void createMapping(HazelcastInstance instance, String name, Class<?> keyClass, Class<?> valueClass) {
        try (SqlResult result = instance.getSql().execute("CREATE OR REPLACE MAPPING " + name + " TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + keyClass.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_CLASS + "'='" + valueClass.getName() + "'\n"
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * portable serialization for both key and value with the given ids.
     */
    public static void createMapping(
            String name,
            int keyFactoryId, int keyClassId, int keyVersion,
            int valueFactoryId, int valueClassId, int valueVersion
    ) {
        createMapping(instance(), name, keyFactoryId, keyClassId, keyVersion, valueFactoryId, valueClassId, valueVersion);
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * portable serialization for both key and value with the given ids.
     */
    public static void createMapping(
            HazelcastInstance instance,
            String name,
            int keyFactoryId, int keyClassId, int keyVersion,
            int valueFactoryId, int valueClassId, int valueVersion
    ) {
        try (SqlResult result = instance.getSql().execute("CREATE OR REPLACE MAPPING " + name + " TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + keyFactoryId + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + keyClassId + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + keyVersion + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + valueFactoryId + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + valueClassId + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + valueVersion + '\''
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for key and portable serialization for value
     * with the given class and ids.
     */
    public static void createMapping(
            String name,
            Class<?> keyClass,
            int valueFactoryId, int valueClassId, int valueVersion
    ) {
        createMapping(instance(), name, keyClass, valueFactoryId, valueClassId, valueVersion);
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for key and portable serialization for value
     * with the given class and ids.
     */
    public static void createMapping(
            HazelcastInstance instance,
            String name,
            Class<?> keyClass,
            int valueFactoryId, int valueClassId, int valueVersion
    ) {
        try (SqlResult result = instance.getSql().execute("CREATE OR REPLACE MAPPING " + name + " TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + keyClass.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + valueFactoryId + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + valueClassId + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + valueVersion + '\''
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
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
        return StringUtil.lowerCaseInternal(System.getProperty("os.name")).contains("windows")
                ? "c:\\non\\existing\\path"
                : "/non/existing/path";
    }

    public static SqlInternalService sqlInternalService(HazelcastInstance instance) {
        return nodeEngine(instance).getSqlService().getInternalService();
    }

    public static PlanCache planCache(HazelcastInstance instance) {
        return nodeEngine(instance).getSqlService().getPlanCache();
    }

    public static NodeEngineImpl nodeEngine(HazelcastInstance instance) {
        return Accessors.getNodeEngineImpl(instance);
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

        public Object[] getValues() {
            return values;
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
