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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import example.serialization.InnerDTO;
import example.serialization.MainDTO;
import example.serialization.NamedDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CompactTestUtil {

    private CompactTestUtil() {

    }

    @Nonnull
    static GenericRecord createCompactGenericRecord(MainDTO mainDTO) {
        InnerDTO inner = mainDTO.p;
        GenericRecord[] namedRecords = new GenericRecord[inner.nn.length];
        int i = 0;
        for (NamedDTO named : inner.nn) {
            GenericRecord namedRecord = GenericRecordBuilder.compact("named")
                    .setString("name", named.name)
                    .setInt("myint", named.myint).build();
            namedRecords[i++] = namedRecord;
        }

        GenericRecord innerRecord = GenericRecordBuilder.compact("inner")
                .setArrayOfBytes("b", inner.bb)
                .setArrayOfShorts("s", inner.ss)
                .setArrayOfInts("i", inner.ii)
                .setArrayOfLongs("l", inner.ll)
                .setArrayOfFloats("f", inner.ff)
                .setArrayOfDoubles("d", inner.dd)
                .setArrayOfGenericRecords("nn", namedRecords)
                .setArrayOfDecimals("bigDecimals", inner.bigDecimals)
                .setArrayOfTimes("localTimes", inner.localTimes)
                .setArrayOfDates("localDates", inner.localDates)
                .setArrayOfTimestamps("localDateTimes", inner.localDateTimes)
                .setArrayOfTimestampWithTimezones("offsetDateTimes", inner.offsetDateTimes)
                .build();

        return GenericRecordBuilder.compact("main")
                .setByte("b", mainDTO.b)
                .setBoolean("bool", mainDTO.bool)
                .setShort("s", mainDTO.s)
                .setInt("i", mainDTO.i)
                .setLong("l", mainDTO.l)
                .setFloat("f", mainDTO.f)
                .setDouble("d", mainDTO.d)
                .setString("str", mainDTO.str)
                .setDecimal("bigDecimal", mainDTO.bigDecimal)
                .setGenericRecord("p", innerRecord)
                .setTime("localTime", mainDTO.localTime)
                .setDate("localDate", mainDTO.localDate)
                .setTimestamp("localDateTime", mainDTO.localDateTime)
                .setTimestampWithTimezone("offsetDateTime", mainDTO.offsetDateTime)
                .setNullableByte("nullable_b", mainDTO.b)
                .setNullableBoolean("nullable_bool", mainDTO.bool)
                .setNullableShort("nullable_s", mainDTO.s)
                .setNullableInt("nullable_i", mainDTO.i)
                .setNullableLong("nullable_l", mainDTO.l)
                .setNullableFloat("nullable_f", mainDTO.f)
                .setNullableDouble("nullable_d", mainDTO.d)
                .build();
    }

    @Nonnull
    static MainDTO createMainDTO() {
        NamedDTO[] nn = new NamedDTO[2];
        nn[0] = new NamedDTO("name", 123);
        nn[1] = new NamedDTO("name", 123);
        InnerDTO inner = new InnerDTO(new boolean[]{true, false}, new byte[]{0, 1, 2},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now()},
                new Boolean[]{true, false, null},
                new Byte[]{0, 1, 2, null},
                new Short[]{3, 4, 5, null}, new Integer[]{9, 8, 7, 6, null}, new Long[]{0L, 1L, 5L, 7L, 9L, 11L},
                new Float[]{0.6543f, -3.56f, 45.67f}, new Double[]{456.456, 789.789, 321.321},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now(), null},
                new LocalDateTime[]{LocalDateTime.now(), null},
                new OffsetDateTime[]{OffsetDateTime.now()});

        return new MainDTO((byte) 113, true, (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now(),
                (byte) 113, true, (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d);
    }

    public static SchemaService createInMemorySchemaService() {
        return new SchemaService() {
            private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();

            @Override
            public Schema get(long schemaId) {
                return schemas.get(schemaId);
            }

            @Override
            public void put(Schema schema) {
                long schemaId = schema.getSchemaId();
                Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
                if (existingSchema != null && !schema.equals(existingSchema)) {
                    throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                            + "existing schema " + existingSchema
                            + "new schema " + schema);
                }
            }

            @Override
            public void putLocal(Schema schema) {
                put(schema);
            }
        };
    }
}
