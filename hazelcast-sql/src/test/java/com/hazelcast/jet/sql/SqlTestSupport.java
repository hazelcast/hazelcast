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

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ExpressionEvalContextImpl;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
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
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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

    public static void setupCompactTypesForNestedQuery(HazelcastInstance instance) {
        instance.getSql().execute("CREATE TYPE Office ("
                + "id BIGINT, "
                + "name VARCHAR "
                + ") OPTIONS ('format'='compact', 'compactTypeName'='OfficeCompactType')");

        instance.getSql().execute("CREATE TYPE Organization ("
                + "id BIGINT, "
                + "name VARCHAR, "
                + "office Office"
                + ") OPTIONS ('format'='compact', 'compactTypeName'='OrganizationCompactType')");

        instance.getSql().execute(
                "CREATE MAPPING test ("
                        + "__key BIGINT,"
                        + "id BIGINT, "
                        + "name VARCHAR, "
                        + "organization Organization"
                        + ")"
                        + "TYPE IMap "
                        + "OPTIONS ("
                        + "'keyFormat'='bigint',"
                        + "'valueFormat'='compact',"
                        + "'valueCompactTypeName'='UserCompactType'"
                        + ")");
    }

    public static void setupPortableTypesForNestedQuery(HazelcastInstance instance) {
        instance.getSql().execute("CREATE TYPE Office OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='3', 'portableClassVersion'='0')");
        instance.getSql().execute("CREATE TYPE Organization OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='2', 'portableClassVersion'='0')");

        instance.getSql().execute("CREATE MAPPING test ("
                + "__key BIGINT, "
                + "id BIGINT, "
                + "name VARCHAR, "
                + "organization Organization "
                + ") TYPE IMap "
                + "OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='portable', "
                + "'valuePortableFactoryId'='1', "
                + "'valuePortableClassId'='1')");
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
     * expectedRows}. Suitable for streaming queries that don't terminate, but
     * return a deterministic set of rows. Rows can arrive in any order.
     * <p>
     * After all expected rows are received, the method further waits a little
     * more if any extra rows are received, and fails, if they are.
     *
     * @param sql          The query
     * @param arguments    The query arguments
     * @param expectedRows Expected rows
     */
    public static void assertRowsEventuallyInAnyOrder(String sql, List<Object> arguments, Collection<Row> expectedRows) {
        assertRowsEventuallyInAnyOrder(sql, arguments, expectedRows, 50);
    }

    /**
     * Assert the contents of a given table via Hazelcast SQL engine
     */
    public static void assertRowsAnyOrder(String sql, Row... rows) {
        assertRowsAnyOrder(sql, Arrays.asList(rows));
    }

    /**
     * Assert the contents of a given table via Hazelcast SQL engine
     */
    public static void assertRowsAnyOrder(String sql, List<Object> arguments, Row... rows) {
        assertRowsAnyOrder(sql, arguments, Arrays.asList(rows));
    }

    public static void assertRowsEventuallyInAnyOrder(
            String sql,
            List<Object> arguments,
            Collection<Row> expectedRows,
            long timeoutForNextMs
    ) {
        assertRowsEventuallyInAnyOrder(instance(), sql, arguments, expectedRows, timeoutForNextMs);
    }

    public static void assertRowsEventuallyInAnyOrder(
            HazelcastInstance instance,
            String sql,
            Collection<Row> expectedRows
    ) {
        assertRowsEventuallyInAnyOrder(instance, sql, emptyList(), expectedRows, 50);
    }

    /**
     * Execute a query and wait for the results to contain all the {@code
     * expectedRows}. Suitable for streaming queries that don't terminate, but
     * return a deterministic set of rows. Rows can arrive in any order.
     * <p>
     * After all expected rows are received, the method further waits a little
     * more if any extra rows are received, and fails, if they are.
     *
     * @param instance         The HZ instance
     * @param sql              The query
     * @param arguments        The query arguments
     * @param expectedRows     Expected rows
     * @param timeoutForNextMs The number of ms to wait for more rows after all the
     *                         expected rows were received
     */
    public static void assertRowsEventuallyInAnyOrder(
            HazelcastInstance instance,
            String sql,
            List<Object> arguments,
            Collection<Row> expectedRows,
            long timeoutForNextMs
    ) {
        SqlService sqlService = instance.getSql();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Deque<Row> rows = new ArrayDeque<>();

        Thread thread = new Thread(() -> {
            SqlStatement statement = new SqlStatement(sql);
            arguments.forEach(statement::addParameter);

            try (SqlResult result = sqlService.execute(statement)) {
                ResultIterator<SqlRow> iterator = (ResultIterator<SqlRow>) result.iterator();
                for (
                        int i = 0;
                        i < expectedRows.size() && (iterator.hasNext()
                                || iterator.hasNext(timeoutForNextMs, TimeUnit.MILLISECONDS) == YES);
                        i++
                ) {
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
     * Execute the given `sql` and assert that the first rows it returns are those in
     * `expectedRows`. Ignores the rest of rows.
     * <p>
     * This is useful for asserting initial output of streaming queries where
     * the output arrives in a stable order.
     */
    public static void assertTipOfStream(String sql, Collection<Row> expectedRows) {
        assert !expectedRows.isEmpty() : "no point in asserting a zero-length tip of a stream";
        SqlService sqlService = instance().getSql();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Deque<Row> rows = new ArrayDeque<>();

        Thread thread = new Thread(() -> {
            SqlStatement statement = new SqlStatement(sql);
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
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    /**
     * Runs a streaming query and checks that for a hard-coded time it
     * doesn't return any results.
     */
    public static void assertEmptyResultStream(String sql) {
        Future<Boolean> future;
        try (SqlResult result = instance().getSql().execute(sql)) {
            future = spawn(() -> result.iterator().hasNext());
            assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
        }
        assertTrueEventually(() -> assertTrue(future.isDone()));
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
     * @param instance     Hazelcast Instance to be used
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertRowsAnyOrder(HazelcastInstance instance, String sql, Collection<Row> expectedRows) {
        assertRowsAnyOrder(instance, sql, emptyList(), expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows does not contain the expected rows, in any order.
     *
     * @param instance     Hazelcast Instance to be used
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertDoesNotContainRow(HazelcastInstance instance, String sql, Collection<Row> expectedRows) {
        SqlStatement statement = new SqlStatement(sql);

        SqlService sqlService = instance.getSql();
        List<Row> actualRows = allRows(statement, sqlService);
        assertThat(actualRows).doesNotContainAnyElementsOf(expectedRows);
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
        assertRowsAnyOrder(instance(), sql, arguments, expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in any order.
     *
     * @param instance     Hazelcast Instance to be used
     * @param sql          The query
     * @param arguments    The query arguments
     * @param expectedRows Expected rows
     */
    public static void assertRowsAnyOrder(
            HazelcastInstance instance,
            String sql,
            List<Object> arguments,
            Collection<Row> expectedRows
    ) {
        SqlStatement statement = new SqlStatement(sql);
        arguments.forEach(statement::addParameter);

        SqlService sqlService = instance.getSql();
        List<Row> actualRows = allRows(statement, sqlService);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @Nonnull
    protected static List<Row> allRows(SqlStatement statement, SqlService sqlService) {
        List<Row> actualRows = new ArrayList<>();
        try (SqlResult result = sqlService.execute(statement)) {
            result.iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        }
        return actualRows;
    }

    @Nonnull
    protected static List<Row> allRows(String statement, SqlService sqlService) {
        try (SqlResult result = sqlService.execute(statement)) {
            return allRows(result);
        }
    }

    @Nonnull
    protected static List<Row> allRows(SqlResult result) {
        List<Row> actualRows = new ArrayList<>();
        result.iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        return actualRows;
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in the given order.
     *
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertRowsOrdered(String sql, List<Row> expectedRows) {
        assertRowsOrdered(instance(), sql, expectedRows);
    }

    /**
     * Execute a query and wait until it completes. Assert that the returned
     * rows contain the expected rows, in the given order.
     *
     * @param instance     Hazelcast Instance to be used
     * @param sql          The query
     * @param expectedRows Expected rows
     */
    public static void assertRowsOrdered(HazelcastInstance instance, String sql, List<Row> expectedRows) {
        SqlService sqlService = instance.getSql();
        List<Row> actualRows = new ArrayList<>();
        try (SqlResult result = sqlService.execute(sql)) {
            result.iterator().forEachRemaining(row -> actualRows.add(new Row(row)));
        }
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
    }

    public static void checkEquals(Object first, Object second, boolean expected) {
        if (expected) {
            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
        } else {
            assertNotEquals(first, second);
        }
    }

    public static <T> T serializeAndCheck(Object original, int expectedClassId) {
        assertTrue(original instanceof IdentifiedDataSerializable);

        IdentifiedDataSerializable original0 = (IdentifiedDataSerializable) original;

        assertEquals(JetSqlSerializerHook.F_ID, original0.getFactoryId());
        assertEquals(expectedClassId, original0.getClassId());

        return serialize(original);
    }

    public static <T> T serialize(Object original) {
        InternalSerializationService ss = serializationService();
        return ss.toObject(ss.toData(original));
    }

    public static InternalSerializationService serializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for both key and value with the given classes.
     */
    public static void createMapping(String name, Class<?> keyClass, Class<?> valueClass) {
        createMapping(instance(), name, keyClass, valueClass);
    }

    public static void createType(String name, Class<?> clazz) {
        createType(instance(), name, clazz);
    }

    public static void createType(HazelcastInstance instance, String name, Class<?> clazz) {
        instance.getSql().execute(format("CREATE TYPE \"%s\" OPTIONS ('format'='java', 'javaClass'='%s')", name, clazz.getName()));
    }

    /**
     * Create an IMap mapping with the given {@code name} that uses
     * java serialization for both key and value with the given classes.
     */
    public static void createMapping(HazelcastInstance instance, String name, Class<?> keyClass, Class<?> valueClass) {
        try (SqlResult result = instance.getSql().execute("CREATE OR REPLACE MAPPING " + name
                + " TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
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

    /**
     * Create an IMap mapping with given {@code name} that uses provided arbitrary key and value formats.
     */
    public static void createMapping(String name, String keyFormat, String valueFormat) {
        createMapping(instance(), name, keyFormat, valueFormat);
    }

    public static void createMapping(HazelcastInstance instance, String name, String keyFormat, String valueFormat) {
        String sql = "CREATE OR REPLACE MAPPING " + name
                + " TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + keyFormat + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + valueFormat + "'\n"
                + ")";
        try (SqlResult result = instance.getSql().execute(sql)) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }

    /**
     * Create an IMap index with given {@code name}, {@code type} and {@code attributes}.
     */
    public static void createIndex(String name, String mapName, IndexType type, String... attributes) {
        createIndex(instance(), name, mapName, type, attributes);
    }

    public static void createIndex(HazelcastInstance instance, String name, String mapName, IndexType type, String... attributes) {
        SqlService sqlService = instance.getSql();

        StringBuilder sb = new StringBuilder("CREATE INDEX IF NOT EXISTS ");
        sb.append(name);
        sb.append(" ON ");
        sb.append(mapName);
        sb.append(" ( ");
        for (int i = 0; i < attributes.length; ++i) {
            if (attributes.length - i - 1 == 0) {
                sb.append(attributes[i]);
            } else {
                sb.append(attributes[i]).append(", ");
            }
        }
        sb.append(" ) ");
        sb.append(" TYPE ");
        sb.append(type.name());

        sqlService.execute(sb.toString());
    }

    public static void createDataConnection(
            HazelcastInstance instance,
            String name,
            String type,
            boolean shared,
            Map<String, String> options
    ) {
        StringBuilder queryBuilder = new StringBuilder()
                .append("CREATE OR REPLACE DATA CONNECTION ")
                .append(quoteName(name))
                .append(" TYPE ")
                .append(quoteName(type))
                .append(shared ? " SHARED " : " NOT SHARED ")
                .append(" OPTIONS (\n");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            queryBuilder.append("'").append(entry.getKey()).append("'")
                    .append(" = ")
                    .append("'").append(entry.getValue()).append("',");
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        queryBuilder.append(")");

        try (SqlResult result = instance.getSql().execute(queryBuilder.toString())) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }

    public static String quoteName(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public static String randomName() {
        // Prefix the UUID with some letters and remove dashes so that it doesn't start with
        // a number and is a valid SQL identifier without quoting.
        return "o_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    /**
     * Compares two lists. The lists are expected to contain elements of type
     * {@link JetSqlRow} or {@link Watermark}.
     * Useful for {@link TestSupport#outputChecker(BiPredicate)}.
     */
    public static boolean compareRowLists(List<?> expected, List<?> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            if (expected.get(i) instanceof JetSqlRow) {
                JetSqlRow expectedItem = (JetSqlRow) expected.get(i);
                JetSqlRow actualItem = (JetSqlRow) actual.get(i);
                if (!Objects.equals(expectedItem, actualItem)) {
                    return false;
                }
            } else if (expected.get(i) instanceof Watermark) {
                Watermark expectedItem = (Watermark) expected.get(i);
                Watermark actualItem = (Watermark) actual.get(i);
                if (!Objects.equals(expectedItem, actualItem)) {
                    return false;
                }
            } else {
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

    public static SqlServiceImpl sqlServiceImpl(HazelcastInstance instance) {
        return (SqlServiceImpl) instance.getSql();
    }

    public static SqlInternalService sqlInternalService(HazelcastInstance instance) {
        return sqlServiceImpl(instance).getInternalService();
    }

    public static PlanCache planCache(HazelcastInstance instance) {
        return sqlServiceImpl(instance).getPlanCache();
    }

    public static MapContainer mapContainer(IMap<?, ?> map) {
        return ((MapProxyImpl<?, ?>) map).getService().getMapServiceContext().getMapContainer(map.getName());
    }

    public static NodeEngineImpl nodeEngine(HazelcastInstance instance) {
        return Accessors.getNodeEngineImpl(instance);
    }

    public static List<Row> rows(final int rowLength, final Object... values) {
        if (rowLength == 0) {
            assertThat(values).isEmpty();
            return emptyList();
        }

        if ((values.length % rowLength) != 0) {
            throw new HazelcastException("Number of row value args is not divisible by row length");
        }

        final List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < values.length; i += rowLength) {
            Object[] rowValues = new Object[rowLength];
            System.arraycopy(values, i, rowValues, 0, rowLength);
            rowList.add(new Row(rowValues));
        }

        return rowList;
    }

    public static LocalTime time(long epochMillis) {
        return timestampTz(epochMillis).toLocalTime();
    }

    public static LocalDate date(long epochMillis) {
        return timestampTz(epochMillis).toLocalDate();
    }

    public static LocalDateTime timestamp(long epochMillis) {
        return timestampTz(epochMillis).toLocalDateTime();
    }

    public static OffsetDateTime timestampTz(long epochMillis) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }

    public static ExpressionEvalContext createExpressionEvalContext(Object... args) {
        if (args == null) {
            args = new Object[0];
        }

        return new ExpressionEvalContextImpl(
                Arrays.asList(args),
                TEST_SS,
                instances() != null ? Util.getNodeEngine(instance()) : mock(NodeEngine.class));
    }

    public static JetSqlRow jetRow(Object... values) {
        return new JetSqlRow(TEST_SS, values);
    }

    protected static Object[] row(Object... values) {
        return values;
    }

    public static class SqlMapping {
        protected final String name;
        protected final String type;
        protected final List<String> fields = new ArrayList<>();
        protected final Map<Object, Object> options = new HashMap<>();

        public SqlMapping(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public SqlMapping fields(String... fields) {
            this.fields.addAll(asList(fields));
            return this;
        }

        public SqlMapping options(Object... options) {
            for (int i = 0; i < options.length / 2; i++) {
                this.options.put(options[2 * i], options[2 * i + 1]);
            }
            return this;
        }

        public SqlMapping optionsIf(boolean condition, Object... options) {
            return condition ? options(options) : this;
        }

        public void create() {
            create(instance(), false);
        }

        public void createOrReplace() {
            create(instance(), true);
        }

        protected void create(HazelcastInstance instance, boolean replace) {
            instance.getSql().execute("CREATE " + (replace ? "OR REPLACE " : "") + "MAPPING " + name
                    + (fields.isEmpty() ? " " : "(" + String.join(",", fields) + ") ")
                    + "TYPE " + type + " "
                    + "OPTIONS (" + options.entrySet().stream()
                            .map(e -> "'" + e.getKey() + "'='" + e.getValue() + "'").collect(joining(","))
                    + ")"
            );
        }
    }

    /**
     * A class passed to utility methods in this class. We don't use SqlRow
     * directly because:
     * - SqlRow doesn't implement `equals`
     * - It's not easy to create SqlRow instance
     */
    public static final class Row {

        private final Object[] values;

        public Row(SqlRow row) {
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
