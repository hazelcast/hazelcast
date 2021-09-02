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

import com.google.common.collect.Iterators;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Verifies that the member can handle queries on Compact when it does not have the necessary classes on the server
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientCompactQueryTest extends HazelcastTestSupport {
    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean clusterHaveUserClasses;

    public TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, clusterHaveUserClasses:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default").setInMemoryFormat(inMemoryFormat);
        Config config = smallInstanceConfig().addMapConfig(mapConfig);
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        if (!clusterHaveUserClasses) {
            List<String> excludes = singletonList("example.serialization");
            FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
            config.setClassLoader(classLoader);
        }
        factory.newHazelcastInstance(config);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testQueryOnPrimitive() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig());
        IMap<Integer, Object> map = client.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new EmployeeDTO(i, i));
        }

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INTEGER"
                + ", age INTEGER"
                + ", \"rank\" INTEGER"
                + ", id BIGINT"
                + ", isHired BOOLEAN"
                + ", isFired BOOLEAN"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + "int" + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + "employee" + '\''
                + ")");

        SqlResult result = client.getSql().execute("SELECT * FROM test WHERE age >= 5");

        assertThat(result).hasSize(5);
    }

    @Test
    @Ignore("see https://github.com/hazelcast/hazelcast/issues/19427")
    public void testQueryOnObject() {
        //To be able to run comparison methods on objects on the server we need the classes
        assumeTrue(clusterHaveUserClasses);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig());
        IMap<Integer, Object> map = client.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new NodeDTO(new NodeDTO(i), i));
        }

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INTEGER"
                + ", id INTEGER"
                + ", child OBJECT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + "int" + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + "employee" + '\''
                + ")");

        SqlResult result = client.getSql().execute("SELECT id FROM test WHERE child = ?", new NodeDTO(new NodeDTO(5), 5));

        SqlRow row = Iterators.getOnlyElement(result.iterator());
        assertEquals(new Integer(5), row.getObject("id"));
    }

    @Test
    public void testNestedCompactAsColumn() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig());
        IMap<Integer, NodeDTO> map = client.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new NodeDTO(new NodeDTO(i), i));
        }

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INTEGER"
                + ", id INTEGER"
                + ", child OBJECT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + "int" + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + "employee" + '\''
                + ")");

        SqlResult result = client.getSql().execute("SELECT id, child FROM test WHERE id = ? ", 1);

        SqlRow row = Iterators.getOnlyElement(result.iterator());
        assertEquals((Integer) 1, row.getObject(0));
        assertEquals(new NodeDTO(1), row.getObject(1));
    }

    @Test
    public void testObjectCanBeUsedForAll() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig());

        boolean[] booleans = {false, true};
        byte[] bytes = {1, 0};
        short[] shorts = {13, 23};
        char[] chars = {'a', 'b'};
        int[] ints = {321, 421};
        long[] longs = {3231L, 1513L};
        float[] floats = {32123.231f, 4343.32f};
        double[] doubles = {3212.3, 413232.1};
        BigDecimal[] decimals = {BigDecimal.valueOf(3213), BigDecimal.valueOf(313)};
        LocalTime[] times = {LocalTime.now()};
        LocalDate[] dates = {LocalDate.now()};
        LocalDateTime[] tmstmps = {LocalDateTime.now()};
        OffsetDateTime[] tmstmptzs = {OffsetDateTime.now()};
        String[] strings = {"hello", "world"};
        boolean bool = true;
        byte b = (byte) 1;
        short s = (short) 13;
        char c = 'a';
        int i = 321;
        long l = 31231L;
        float f = 32123.231f;
        double d = 3212.3;
        BigDecimal dl = BigDecimal.valueOf(3213);
        String st = "test";
        LocalTime t = LocalTime.now();
        LocalDate dt = LocalDate.now();
        LocalDateTime tmstmp = LocalDateTime.now();
        OffsetDateTime tmstmptz = OffsetDateTime.now();

        client.getSql().execute("CREATE MAPPING " + "test" + '('
                + "__key INT "
                + ", bool OBJECT "
                + ", b OBJECT"
                + ", s OBJECT"
                + ", c OBJECT"
                + ", i OBJECT"
                + ", l OBJECT"
                + ", f OBJECT"
                + ", d OBJECT"
                + ", st OBJECT"
                + ", dl OBJECT"
                + ", t OBJECT"
                + ", dt OBJECT"
                + ", tmstmp OBJECT"
                + ", tmstmptz OBJECT"
                + ", booleans OBJECT "
                + ", bytes OBJECT"
                + ", shorts OBJECT"
                + ", chars OBJECT"
                + ", ints OBJECT"
                + ", longs OBJECT"
                + ", floats OBJECT"
                + ", doubles OBJECT"
                + ", strings OBJECT"
                + ", decimals OBJECT"
                + ", times OBJECT"
                + ", dates OBJECT"
                + ", tmstmps OBJECT"
                + ", tmstmptzs OBJECT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + "int" + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + "testObject" + '\''
                + ")"
        );

        client.getSql().execute("SINK INTO test "
                        + "( __key, bool, b, s, c, i, l, f, d, st, dl, t, dt, tmstmp, tmstmptz"
                        + ", booleans, bytes, shorts, chars, ints, longs, floats, doubles, strings,"
                        + " decimals, times, dates, tmstmps, tmstmptzs)"
                        + " VALUES (? "
                        + ",CAST (? AS BOOLEAN)"
                        + ",CAST (? AS TINYINT)"
                        + ",CAST (? AS SMALLINT)"
                        + ",CAST (? AS VARCHAR)"
                        + ",CAST (? AS INTEGER)"
                        + ",CAST (? AS BIGINT)"
                        + ",CAST (? AS REAL)"
                        + ",CAST (? AS DOUBLE)"
                        + ",CAST (? AS VARCHAR)"
                        + ",CAST (? AS DECIMAL)"
                        + ",CAST (? AS TIME)"
                        + ",CAST (? AS DATE)"
                        + ",CAST (? AS TIMESTAMP)"
                        + ",CAST (? AS TIMESTAMP WITH TIME ZONE)"
                        + ",?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ",
                1, bool, b, s, c, i, l, f, d, st, dl, t, dt, tmstmp, tmstmptz,
                booleans, bytes, shorts, chars, ints, longs, floats, doubles, strings,
                decimals, times, dates, tmstmps, tmstmptzs);


        SqlResult result = client.getSql().execute("SELECT * FROM test");
        SqlRow row = Iterators.getOnlyElement(result.iterator());
        assertEquals(bool, row.getObject("bool"));
        assertEquals((Byte) b, row.getObject("b"));
        assertEquals((Short) s, row.getObject("s"));
        assertEquals(String.valueOf((Character) c), row.getObject("c"));
        assertEquals((Integer) i, row.getObject("i"));
        assertEquals((Long) l, row.getObject("l"));
        assertEquals(f, row.getObject("f"), 0);
        assertEquals(d, row.getObject("d"), 0);
        assertEquals(dl, row.getObject("dl"));
        assertEquals(st, row.getObject("st"));
        assertEquals(t, row.getObject("t"));
        assertEquals(dt, row.getObject("dt"));
        assertEquals(tmstmp, row.getObject("tmstmp"));
        assertEquals(tmstmptz, row.getObject("tmstmptz"));

        assertArrayEquals(booleans, row.getObject("booleans"));
        assertArrayEquals(bytes, row.getObject("bytes"));
        assertArrayEquals(shorts, row.getObject("shorts"));
        assertArrayEquals(chars, row.getObject("chars"));
        assertArrayEquals(ints, row.getObject("ints"));
        assertArrayEquals(longs, row.getObject("longs"));
        assertArrayEquals(floats, row.getObject("floats"), 0);
        assertArrayEquals(doubles, row.getObject("doubles"), 0);
        assertArrayEquals(decimals, row.getObject("decimals"));
        assertArrayEquals(strings, row.getObject("strings"));
        assertArrayEquals(times, row.getObject("times"));
        assertArrayEquals(dates, row.getObject("dates"));
        assertArrayEquals(tmstmps, row.getObject("tmstmps"));
        assertArrayEquals(tmstmptzs, row.getObject("tmstmptzs"));
    }

    private static ClientConfig clientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        return clientConfig;
    }
}
