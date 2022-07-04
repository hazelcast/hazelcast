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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class AvroUpsertTargetTest {

    @Test
    public void test_set() {
        Schema schema = SchemaBuilder.record("name")
                .fields()
                .name("null").type().nullType().nullDefault()
                .name("string").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("boolean").type().unionOf().nullType().and().booleanType().endUnion().nullDefault()
                .name("byte").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                .name("short").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                .name("int").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                .name("long").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .name("float").type().unionOf().nullType().and().floatType().endUnion().nullDefault()
                .name("double").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
                .name("decimal").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("time").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("date").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("timestamp").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("timestampTz").type()
                        .unionOf()
                        .nullType()
                        .and().booleanType()
                        .and().intType()
                        .and().longType()
                        .and().floatType()
                        .and().doubleType()
                        .and().stringType()
                        .endUnion().nullDefault()
                .endRecord();

        UpsertTarget target = new AvroUpsertTarget(schema.toString());
        UpsertInjector nullInjector = target.createInjector("null", QueryDataType.OBJECT);
        UpsertInjector stringInjector = target.createInjector("string", QueryDataType.VARCHAR);
        UpsertInjector booleanInjector = target.createInjector("boolean", QueryDataType.BOOLEAN);
        UpsertInjector byteInjector = target.createInjector("byte", QueryDataType.TINYINT);
        UpsertInjector shortInjector = target.createInjector("short", QueryDataType.SMALLINT);
        UpsertInjector intInjector = target.createInjector("int", QueryDataType.INT);
        UpsertInjector longInjector = target.createInjector("long", QueryDataType.BIGINT);
        UpsertInjector floatInjector = target.createInjector("float", QueryDataType.REAL);
        UpsertInjector doubleInjector = target.createInjector("double", QueryDataType.DOUBLE);
        UpsertInjector decimalInjector = target.createInjector("decimal", QueryDataType.DECIMAL);
        UpsertInjector timeInjector = target.createInjector("time", QueryDataType.TIME);
        UpsertInjector dateInjector = target.createInjector("date", QueryDataType.DATE);
        UpsertInjector timestampInjector = target.createInjector("timestamp", QueryDataType.TIMESTAMP);
        UpsertInjector timestampTzInjector =
                target.createInjector("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);

        target.init();
        nullInjector.set(null);
        stringInjector.set("string");
        booleanInjector.set(true);
        byteInjector.set((byte) 127);
        shortInjector.set((short) 32767);
        intInjector.set(2147483647);
        longInjector.set(9223372036854775807L);
        floatInjector.set(1234567890.1F);
        doubleInjector.set(123451234567890.1D);
        decimalInjector.set(new BigDecimal("9223372036854775.123"));
        timeInjector.set(LocalTime.of(12, 23, 34));
        dateInjector.set(LocalDate.of(2020, 9, 9));
        timestampInjector.set(LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000));
        timestampTzInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC));
        Object record = target.conclude();

        assertThat(record).isEqualTo(new GenericRecordBuilder(schema)
                .set("null", null)
                .set("string", "string")
                .set("boolean", true)
                .set("byte", 127)
                .set("short", 32767)
                .set("int", 2147483647)
                .set("long", 9223372036854775807L)
                .set("float", 1234567890.1F)
                .set("double", 123451234567890.1D)
                .set("decimal", "9223372036854775.123")
                .set("time", "12:23:34")
                .set("date", "2020-09-09")
                .set("timestamp", "2020-09-09T12:23:34.100")
                .set("timestampTz", "2020-09-09T12:23:34.200Z")
                .build()
        );
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{null, null},
                new Object[]{"string", "string"},
                new Object[]{true, true},
                new Object[]{(byte) 127, 127},
                new Object[]{(short) 32767, 32767},
                new Object[]{2147483647, 2147483647},
                new Object[]{9223372036854775807L, 9223372036854775807L},
                new Object[]{1234567890.1F, 1234567890.1F},
                new Object[]{123451234567890.1D, 123451234567890.1D},
                new Object[]{new BigDecimal("9223372036854775.123"), "9223372036854775.123"},
                new Object[]{LocalTime.of(12, 23, 34), "12:23:34"},
                new Object[]{LocalDate.of(2020, 9, 9), "2020-09-09"},
                new Object[]{LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000), "2020-09-09T12:23:34.100"},
                new Object[]{OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC), "2020-09-09T12:23:34.200Z"},
        };
    }

    @Test
    @Parameters(method = "values")
    public void when_typeIsObject_then_allValuesAreAllowed(Object value, Object expected) {
        Schema schema = SchemaBuilder.record("name")
                                     .fields()
                                     .name("object").type()
                                     .unionOf()
                                     .nullType()
                                     .and().booleanType()
                                     .and().intType()
                                     .and().longType()
                                     .and().floatType()
                                     .and().doubleType()
                                     .and().stringType()
                                     .endUnion().nullDefault()
                                     .endRecord();

        UpsertTarget target = new AvroUpsertTarget(schema.toString());
        UpsertInjector injector = target.createInjector("object", QueryDataType.OBJECT);

        target.init();
        injector.set(value);
        Object record = target.conclude();

        assertThat(record).isEqualTo(new GenericRecordBuilder(schema).set("object", expected).build());
    }
}
