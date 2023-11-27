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
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.mapstore.GenericMapLoader.SINGLE_COLUMN_AS_VALUE;
import static com.hazelcast.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static java.time.OffsetDateTime.of;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesGenericMapStoreTest extends JdbcSqlTestSupport {

    @Parameterized.Parameter()
    public String type;

    @Parameterized.Parameter(1)
    public String value;

    @Parameterized.Parameter(2)
    public Object singleColumn;

    @Parameterized.Parameter(3)
    public BiConsumer<GenericRecord, String> verify;

    @Parameterized.Parameter(4)
    public Consumer<Tuple3<GenericRecordBuilder, String, Object>> setField;

    @Parameterized.Parameter(5)
    public Consumer<Object> verifySingleColumn;


    private String tableName;
    private String tableNameSingleColumn;
    private GenericMapStore<Integer, GenericRecord> mapStore;
    private GenericMapStore<Integer, Object> mapStoreSingleColumn;

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, javaValue:{2} value:{3}, expected:{4}, valueForSingleColumn:{5}")
    @SuppressWarnings("all")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"VARCHAR(100)", "'dummy'", "dummy", (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getString(s)).isEqualTo("dummy"),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setString(tuple3.f1(), (String) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((String) o).isEqualTo("dummy")},
                {"BOOLEAN", "TRUE", true,  (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getBoolean(s)).isTrue(),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setBoolean(tuple3.f1(), (Boolean) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((Boolean) o).isTrue()},
                {"TINYINT", "1", (byte) 1, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt8(s)).isEqualTo((byte) 1),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setInt8(tuple3.f1(), (byte) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((byte) o).isEqualTo((byte) 1)},
                {"SMALLINT", "2", (short) 2, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt16(s)).isEqualTo((short) 2),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setInt16(tuple3.f1(), (short) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((short) o).isEqualTo((short) 2)},
                {"INTEGER", "3", 3, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt32(s)).isEqualTo(3),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setInt32(tuple3.f1(), (Integer) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((Integer) o).isEqualTo(3)},
                {"BIGINT", "4", 4L, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getInt64(s)).isEqualTo(4L),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setInt64(tuple3.f1(), (Long) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((Long) o).isEqualTo(4L)},
                {"DECIMAL (10,5)", "1.12345", new BigDecimal("1.12345"), (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getDecimal(s)).isEqualTo(new BigDecimal("1.12345")),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setDecimal(tuple3.f1(), (BigDecimal) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((BigDecimal) o).isEqualTo(new BigDecimal("1.12345"))},
                {"REAL", "1.5", 1.5f, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getFloat32(s)).isEqualTo(1.5f),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setFloat32(tuple3.f1(), (Float) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((Float) o).isEqualTo(1.5f)},
                {"DOUBLE", "1.8", 1.8, (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getFloat64(s)).isEqualTo(1.8),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setFloat64(tuple3.f1(), (Double) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((Double) o).isEqualTo(1.8)},
                {"DATE", "'2022-12-30'", LocalDate.of(2022, 12, 30), (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getDate(s)).isEqualTo(LocalDate.of(2022, 12, 30)),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setDate(tuple3.f1(), (LocalDate) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((LocalDate) o).isEqualTo(LocalDate.of(2022, 12, 30))},
                {"TIME", "'23:59:59'", LocalTime.of(23, 59, 59), (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getTime(s)).isEqualTo(LocalTime.of(23, 59, 59)),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setTime(tuple3.f1(), (LocalTime) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((LocalTime) o).isEqualTo(LocalTime.of(23, 59, 59))},
                {"TIMESTAMP", "'2022-12-30 23:59:59'", LocalDateTime.of(2022, 12, 30, 23, 59, 59), (BiConsumer<GenericRecord, String>)
                        (r, s) -> assertThat(r.getTimestamp(s)).isEqualTo(LocalDateTime.of(2022, 12, 30, 23, 59, 59)),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setTimestamp(tuple3.f1(), (LocalDateTime) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((LocalDateTime) o).isEqualTo(LocalDateTime.of(2022, 12, 30, 23, 59, 59))},
                {"TIMESTAMP WITH TIME ZONE", "'2022-12-30 23:59:59 -05:00'", of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5)),
                        (BiConsumer<GenericRecord, String>)
                                (r, s) -> assertThat(r.getTimestampWithTimezone(s)).isEqualTo(
                                        of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5))
                                ),
                        (Consumer<Tuple3<GenericRecordBuilder, String, Object>>) (tuple3) -> tuple3.f0().setTimestampWithTimezone(tuple3.f1(), (OffsetDateTime) tuple3.f2()),
                        (Consumer<Object>) (o) -> assertThat((OffsetDateTime) o).isEqualTo(of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5)))},
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

        tableNameSingleColumn = randomTableName();
        createTable(tableNameSingleColumn, "id INT PRIMARY KEY", "table_column " + type);

        Properties propertiesSingleCol = new Properties();
        propertiesSingleCol.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        propertiesSingleCol.setProperty(SINGLE_COLUMN_AS_VALUE, "true");

        MapStoreConfig mapStoreConfigSingleCol = new MapStoreConfig();
        mapStoreConfigSingleCol.setClassName(GenericMapStore.class.getName());
        mapStoreConfigSingleCol.setProperties(propertiesSingleCol);

        MapConfig mapConfigSingleCol = new MapConfig(tableNameSingleColumn);
        mapConfigSingleCol.setMapStoreConfig(mapStoreConfigSingleCol);

        instance().getConfig().addMapConfig(mapConfigSingleCol);

        mapStoreSingleColumn = new GenericMapStore<>();
        mapStoreSingleColumn.init(instance(), propertiesSingleCol, tableNameSingleColumn);
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
        Tuple3<GenericRecordBuilder, String, Object> tuple3 = tuple3(builder, "table_column", singleColumn);
        setField.accept(tuple3);
        mapStore.store(0, builder.build());

        assertThat(jdbcRows("SELECT id FROM " + tableName + " WHERE table_column = " + value))
                .containsExactlyInAnyOrder(new Row(0));
    }

    @Test
    public void storeSingleColumn() {
        mapStoreSingleColumn.store(0, singleColumn);

        assertThat(jdbcRows("SELECT id FROM " + tableNameSingleColumn + " WHERE table_column = " + value))
                .containsExactlyInAnyOrder(new Row(0));
    }

    @Test
    public void loadSingleColumn() throws Exception {
        executeJdbc("INSERT INTO " + tableNameSingleColumn + " VALUES(0, " + value + ")");

        Object column = mapStoreSingleColumn.load(0);
        verifySingleColumn.accept(column);

    }

}
