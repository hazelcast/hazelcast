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

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.InnerDTO;
import example.serialization.MainDTO;
import example.serialization.NamedDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Nonnull
    private GenericRecord createCompactGenericRecord(MainDTO mainDTO) {
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
                .setByteArray("b", inner.bb)
                .setCharArray("c", inner.cc)
                .setShortArray("s", inner.ss)
                .setIntArray("i", inner.ii)
                .setLongArray("l", inner.ll)
                .setFloatArray("f", inner.ff)
                .setDoubleArray("d", inner.dd)
                .setGenericRecordArray("nn", namedRecords)
                .setDecimalArray("bigDecimals", inner.bigDecimals)
                .setTimeArray("localTimes", inner.localTimes)
                .setDateArray("localDates", inner.localDates)
                .setTimestampArray("localDateTimes", inner.localDateTimes)
                .setTimestampWithTimezoneArray("offsetDateTimes", inner.offsetDateTimes)
                .build();

        return GenericRecordBuilder.compact("main")
                .setByte("b", mainDTO.b)
                .setUnsignedByte("ub", mainDTO.ub)
                .setBoolean("bool", mainDTO.bool)
                .setChar("c", mainDTO.c)
                .setShort("s", mainDTO.s)
                .setUnsignedShort("us", mainDTO.us)
                .setInt("i", mainDTO.i)
                .setUnsignedInt("ui", mainDTO.ui)
                .setLong("l", mainDTO.l)
                .setUnsignedLong("ul", mainDTO.ul)
                .setFloat("f", mainDTO.f)
                .setDouble("d", mainDTO.d)
                .setString("str", mainDTO.str)
                .setGenericRecord("p", innerRecord)
                .setDecimal("bigDecimal", mainDTO.bigDecimal)
                .setTime("localTime", mainDTO.localTime)
                .setDate("localDate", mainDTO.localDate)
                .setTimestamp("localDateTime", mainDTO.localDateTime)
                .setTimestampWithTimezone("offsetDateTime", mainDTO.offsetDateTime)
                .build();
    }

    @Nonnull
    private MainDTO createMainDTO() {
        NamedDTO[] nn = new NamedDTO[2];
        nn[0] = new NamedDTO("name", 123);
        nn[1] = new NamedDTO("name", 123);
        InnerDTO inner = new InnerDTO(new byte[]{0, 1, 2}, new int[]{128, 129, 170}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{Short.MAX_VALUE + 1, Short.MAX_VALUE + 2, Short.MAX_VALUE + 3},
                new int[]{9, 8, 7, 6},
                new long[]{(long) Integer.MAX_VALUE + 1, (long) Integer.MAX_VALUE + 2, (long) Integer.MAX_VALUE + 3},
                new long[]{0, 1, 5, 7, 9, 11},
                new BigInteger[]{new BigInteger("9223372036854775808"), new BigInteger("9223372036854775809")},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now()});

        return new MainDTO((byte) 113, 180, true, 'x', (short) -500, Short.MAX_VALUE + 10, 56789,
                (long) Integer.MAX_VALUE + 20, -50992225L, new BigInteger("9223372036854775808"), 900.5678f,
                -897543.3678909d, "this is main object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now());
    }


    @Test
    public void testGenericRecordToStringValidJson() throws IOException {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true).register(MainDTO.class, "MainDTO", new MainDTO.MainDTOSerializer());
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();

        MainDTO expectedDTO = createMainDTO();
        Data data = serializationService.toData(expectedDTO);
        assertTrue(data.isCompact());

        //internal generic record created on the servers on query
        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);
        String string = internalGenericRecord.toString();
        Json.parse(string);

        //generic record read from a remote instance without classes on the classpath
        List<String> excludes = Collections.singletonList("example.serialization");
        FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            InternalSerializationService ss2 = new DefaultSerializationServiceBuilder()
                    .setSchemaService(schemaService)
                    .setClassLoader(classLoader)
                    .setConfig(new SerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig()))
                    .build();
            GenericRecord genericRecord = ss2.toObject(data);
            Json.parse(genericRecord.toString());

            //generic record build by API
            GenericRecord apiGenericRecord = createCompactGenericRecord(expectedDTO);
            Json.parse(apiGenericRecord.toString());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Test
    public void testCloneObjectConvertedFromData() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        GenericRecordBuilder cloneBuilder = genericRecord.cloneWithBuilder();
        cloneBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, cloneBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, cloneBuilder).startsWith("Invalid field name"));

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt("foo"));
        assertEquals(1231L, clone.getLong("bar"));
    }

    private SerializationService createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    @Test
    public void testCloneObjectCreatedViaAPI() {
        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord genericRecord = builder.build();

        GenericRecordBuilder cloneBuilder = genericRecord.cloneWithBuilder();
        cloneBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, cloneBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, cloneBuilder).startsWith("Invalid field name"));

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt("foo"));
        assertEquals(1231L, clone.getLong("bar"));
    }

    @Test
    public void testBuildFromObjectConvertedFromData() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, recordBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, recordBuilder).startsWith("Invalid field name"));
        assertTrue(tryBuildAndGetMessage(recordBuilder).startsWith("Found an unset field"));

        recordBuilder.setLong("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt("foo"));
        assertEquals(100, newRecord.getLong("bar"));
    }

    @Test
    public void testBuildFromObjectCreatedViaAPI() {
        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord genericRecord = builder.build();

        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, recordBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, recordBuilder).startsWith("Invalid field name"));
        assertTrue(tryBuildAndGetMessage(recordBuilder).startsWith("Found an unset field"));

        recordBuilder.setLong("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt("foo"));
        assertEquals(100, newRecord.getLong("bar"));
    }

    private String trySetAndGetMessage(String fieldName, int value, GenericRecordBuilder cloneBuilder) {
        try {
            cloneBuilder.setInt(fieldName, value);
        } catch (HazelcastSerializationException e) {
            return e.getMessage();
        }
        return null;
    }

    private String tryBuildAndGetMessage(GenericRecordBuilder builder) {
        try {
            builder.build();
        } catch (HazelcastSerializationException e) {
            return e.getMessage();
        }
        return null;
    }
}
