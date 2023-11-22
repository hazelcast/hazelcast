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

package com.hazelcast.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Properties;
import java.util.function.BiConsumer;

import static com.hazelcast.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static java.time.OffsetDateTime.of;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({ParallelJVMTest.class})
public class AllTypesGenericMapStoreTest extends JdbcSqlTestSupport {

    @Parameterized.Parameter()
    public String type;

    @Parameterized.Parameter(1)
    public String value;

    @Parameterized.Parameter(2)
    public BiConsumer<GenericRecord, String> verify;

    @Parameterized.Parameter(3)
    public BiConsumer<GenericRecordBuilder, String> setField;

    private String tableName;
    private GenericMapStore<Integer> mapStore;

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, value:{2}, expected:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"VARCHAR(100)", "'dummy'", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getString(s)).isEqualTo("dummy"),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setString(s, "dummy")},
                {"BOOLEAN", "TRUE", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getBoolean(s)).isTrue(),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setBoolean(s, true)},
                {"TINYINT", "1", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt8(s)).isEqualTo((byte) 1),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setInt8(s, (byte) 1)},
                {"SMALLINT", "2", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt16(s)).isEqualTo((short) 2),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setInt16(s, (short) 2)},
                {"INTEGER", "3", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt32(s)).isEqualTo(3),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setInt32(s, 3)},
                {"BIGINT", "4", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt64(s)).isEqualTo(4L),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setInt64(s, 4L)},
                {"DECIMAL (10,5)", "1.12345", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getDecimal(s)).isEqualTo(new BigDecimal("1.12345")),
                        (BiConsumer<GenericRecordBuilder, String>)
                                (rb, s) -> rb.setDecimal(s, new BigDecimal("1.12345"))},
                {"REAL", "1.5", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getFloat32(s)).isEqualTo(1.5f),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setFloat32(s, 1.5f)},
                {"DOUBLE", "1.8", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getFloat64(s)).isEqualTo(1.8),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setFloat64(s, 1.8)},
                {"DATE", "'2022-12-30'", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getDate(s)).isEqualTo(LocalDate.of(2022, 12, 30)),
                        (BiConsumer<GenericRecordBuilder, String>)
                                (rb, s) -> rb.setDate(s, LocalDate.of(2022, 12, 30))},
                {"TIME", "'23:59:59'", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getTime(s)).isEqualTo(LocalTime.of(23, 59, 59)),
                        (BiConsumer<GenericRecordBuilder, String>) (rb, s) -> rb.setTime(s, LocalTime.of(23, 59, 59))},
                {"TIMESTAMP", "'2022-12-30 23:59:59'", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getTimestamp(s)).isEqualTo(LocalDateTime.of(2022, 12, 30, 23, 59, 59)),
                        (BiConsumer<GenericRecordBuilder, String>)
                                (rb, s) -> rb.setTimestamp(s, LocalDateTime.of(2022, 12, 30, 23, 59, 59))},
                {"TIMESTAMP WITH TIME ZONE", "'2022-12-30 23:59:59 -05:00'",
                        (BiConsumer<GenericRecord, String>)
                                (r, s) -> assertThat(r.getTimestampWithTimezone(s)).isEqualTo(
                                        of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5))
                                ),
                        (BiConsumer<GenericRecordBuilder, String>)
                                (rb, s) -> rb.setTimestampWithTimezone(s,
                                        of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5))
                                )},
        });
    }

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();

        initialize(new H2DatabaseProvider(), config);
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        createTable(tableName, "id INT PRIMARY KEY", "table_column " + type);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperties(properties);

        MapConfig mapConfig = new MapConfig(tableName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        instance().getConfig().addMapConfig(mapConfig);



        mapStore = new GenericMapStore<>();
        mapStore.init(instance(), properties, tableName);
    }

    @Test
    public void loadRecord() throws Exception {
        executeJdbc("INSERT INTO " + tableName + " VALUES(0, " + value + ")");

        GenericRecord record = mapStore.load(0);
        verify.accept(record, "table_column");
    }

    @Test
    public void storeRecord() {
        GenericRecordBuilder builder = GenericRecordBuilder.compact("Person");
        builder.setInt32("id", 0);
        setField.accept(builder, "table_column");
        mapStore.store(0, builder.build());

        assertThat(jdbcRows("SELECT id FROM " + tableName + " WHERE table_column = " + value))
                .containsExactlyInAnyOrder(new Row(0));
    }
}
