/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.portable.integration;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.internal.serialization.impl.portable.InnerPortable;
import com.hazelcast.internal.serialization.impl.portable.MainPortable;
import com.hazelcast.internal.serialization.impl.portable.NamedPortable;
import com.hazelcast.internal.serialization.impl.portable.PortableTest;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractGenericRecordIntegrationTest extends HazelcastTestSupport {

    private final SerializationConfig serializationConfig = new SerializationConfig()
            .addPortableFactory(PortableTest.PORTABLE_FACTORY_ID, new PortableTest.TestPortableFactory());

    /**
     * @return instance(client / member) with given serialization config
     */
    protected abstract HazelcastInstance createAccessorInstance(SerializationConfig serializationConfig);

    /**
     * @return cluster without any Portable Factory Config
     */
    protected abstract HazelcastInstance[] createCluster();

    @Test
    public void testToStringIsValidJson() {
        MainPortable expectedPortable = createMainPortable();
        GenericRecord expected = createGenericRecord(expectedPortable);
        Json.parse(expected.toString());
    }

    @Test
    public void testPutWithoutFactory_readAsPortable() {
        MainPortable expectedPortable = createMainPortable();
        GenericRecord expected = createGenericRecord(expectedPortable);

        assertEquals(expectedPortable.c, expected.getChar("c"));
        assertEquals(expectedPortable.f, expected.getFloat32("f"), 0.1);
        HazelcastInstance[] instances = createCluster();
        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        clusterMap.put(1, expected);
        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        MainPortable actual = (MainPortable) map.get(1);
        assertEquals(expectedPortable, actual);
    }

    @Test
    public void testPutWithoutFactory_readAsGenericRecord() {
        MainPortable expectedPortable = createMainPortable();
        GenericRecord expected = createGenericRecord(expectedPortable);

        assertEquals(expectedPortable.c, expected.getChar("c"));
        assertEquals(expectedPortable.f, expected.getFloat32("f"), 0.1);
        HazelcastInstance[] instances = createCluster();
        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        clusterMap.put(1, expected);
        HazelcastInstance instance = createAccessorInstance(new SerializationConfig());
        IMap<Object, Object> map = instance.getMap("test");

        GenericRecord actual = (GenericRecord) map.get(1);
        assertEquals(expected, actual);
    }

    @Test
    public void testPutGenericRecordBack() {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);
        map.put(1, expected);

        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        GenericRecord record = (GenericRecord) clusterMap.get(1);

        clusterMap.put(2, record);

        //read from the cluster without serialization config
        GenericRecord actualRecord = (GenericRecord) clusterMap.get(2);

        assertTrue(actualRecord.hasField("name"));
        assertTrue(actualRecord.hasField("myint"));

        assertEquals(expected.name, actualRecord.getString("name"));
        assertEquals(expected.myint, actualRecord.getInt32("myint"));


        //read from the instance with serialization config
        NamedPortable actualPortable = (NamedPortable) map.get(2);
        assertEquals(expected, actualPortable);
    }

    @Test
    public void testReadReturnsGenericRecord() {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);
        map.put(1, expected);

        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        GenericRecord actual = (GenericRecord) clusterMap.get(1);

        assertTrue(actual.hasField("name"));
        assertTrue(actual.hasField("myint"));

        assertEquals(expected.name, actual.getString("name"));
        assertEquals(expected.myint, actual.getInt32("myint"));
    }

    @Test
    public void testEntryProcessorReturnsGenericRecord() {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);

        String key = generateKeyOwnedBy(instances[0]);
        map.put(key, expected);
        Object returnValue = map.executeOnKey(key, (EntryProcessor<Object, Object, Object>) entry -> {
            Object value = entry.getValue();
            GenericRecord genericRecord = (GenericRecord) value;

            GenericRecord modifiedGenericRecord = genericRecord.newBuilder()
                    .setString("name", "bar")
                    .setInt32("myint", 4).build();

            entry.setValue(modifiedGenericRecord);

            return genericRecord.getInt32("myint");
        });
        assertEquals(expected.myint, returnValue);

        NamedPortable actualPortable = (NamedPortable) map.get(key);
        assertEquals("bar", actualPortable.name);
        assertEquals(4, actualPortable.myint);
    }

    @Test
    public void testCloneWithGenericBuilderOnEntryProcessor() {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);

        String key = generateKeyOwnedBy(instances[0]);
        map.put(key, expected);
        Object returnValue = map.executeOnKey(key, (EntryProcessor<Object, Object, Object>) entry -> {
            Object value = entry.getValue();
            GenericRecord genericRecord = (GenericRecord) value;

            GenericRecord modifiedGenericRecord = genericRecord.cloneWithBuilder()
                    .setInt32("myint", 4).build();

            entry.setValue(modifiedGenericRecord);

            return genericRecord.getInt32("myint");
        });
        assertEquals(expected.myint, returnValue);

        NamedPortable actualPortable = (NamedPortable) map.get(key);
        assertEquals("foo", actualPortable.name);
        assertEquals(4, actualPortable.myint);
    }

    private static class GetInt implements Callable<Integer>, HazelcastInstanceAware, Serializable {

        volatile HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }

        @Override
        public Integer call() throws Exception {
            IMap<Object, Object> map = instance.getMap("test");
            GenericRecord genericRecord = (GenericRecord) map.get(1);
            return genericRecord.getInt32("myint");
        }
    }

    @Test
    public void testGenericRecordIsReturnedInRemoteLogic() throws Exception {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);

        IExecutorService service = instance.getExecutorService("test");

        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);
        map.put(1, expected);

        Future<Integer> actual = service.submitToMember(new GetInt(), instances[0].getCluster().getLocalMember());
        assertEquals(expected.myint, actual.get().intValue());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testInconsistentClassDefinition() {
        createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        BiTuple<GenericRecord, GenericRecord> records = getInconsistentGenericRecords();
        map.put(1, records.element1);
        map.put(2, records.element2);
    }

    @Test
    public void testInconsistentClassDefinition_whenCheckClassDefErrorsIsFalse() {
        createCluster();

        SerializationConfig serializationConfig = new SerializationConfig(this.serializationConfig);
        serializationConfig.setCheckClassDefErrors(false);
        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        BiTuple<GenericRecord, GenericRecord> records = getInconsistentGenericRecords();
        map.put(1, records.element1);
        map.put(2, records.element2);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testInconsistentClassDefinitionOfNestedPortableFields() {
        createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        BiTuple<GenericRecord, GenericRecord> records = getInconsistentNestedGenericRecords();
        map.put(1, records.element1);
        map.put(2, records.element2);
    }

    @Test
    public void testInconsistentClassDefinitionOfNestedPortableFields_whenCheckClassDefErrorsIsFalse() {
        createCluster();

        SerializationConfig serializationConfig = new SerializationConfig(this.serializationConfig);
        serializationConfig.setCheckClassDefErrors(false);
        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        BiTuple<GenericRecord, GenericRecord> records = getInconsistentNestedGenericRecords();
        map.put(1, records.element1);
        map.put(2, records.element2);
    }

    @Nonnull
    private MainPortable createMainPortable() {
        NamedPortable[] nn = new NamedPortable[2];
        nn[0] = new NamedPortable("name", 123);
        nn[1] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), LocalTime.now()},
                new LocalDate[]{LocalDate.now(), LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now()},
                new OffsetDateTime[]{OffsetDateTime.now()});

        return new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now());
    }

    @Nonnull
    private GenericRecord createGenericRecord(MainPortable expectedPortable) {
        InnerPortable inner = expectedPortable.p;
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();
        ClassDefinition innerPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.INNER_PORTABLE)
                        .addByteArrayField("b")
                        .addCharArrayField("c")
                        .addShortArrayField("s")
                        .addIntArrayField("i")
                        .addLongArrayField("l")
                        .addFloatArrayField("f")
                        .addDoubleArrayField("d")
                        .addPortableArrayField("nn", namedPortableClassDefinition)
                        .addDecimalArrayField("bigDecimals")
                        .addTimeArrayField("localTimes")
                        .addDateArrayField("localDates")
                        .addTimestampArrayField("localDateTimes")
                        .addTimestampWithTimezoneArrayField("offsetDateTimes")
                        .build();
        ClassDefinition mainPortableClassDefinition =
                new ClassDefinitionBuilder(PortableTest.PORTABLE_FACTORY_ID, TestSerializationConstants.MAIN_PORTABLE)
                        .addByteField("b")
                        .addBooleanField("bool")
                        .addCharField("c")
                        .addShortField("s")
                        .addIntField("i")
                        .addLongField("l")
                        .addFloatField("f")
                        .addDoubleField("d")
                        .addStringField("str")
                        .addPortableField("p", innerPortableClassDefinition)
                        .addDecimalField("bigDecimal")
                        .addTimeField("localTime")
                        .addDateField("localDate")
                        .addTimestampField("localDateTime")
                        .addTimestampWithTimezoneField("offsetDateTime")
                        .build();

        GenericRecord[] namedRecords = new GenericRecord[inner.nn.length];
        int i = 0;
        for (NamedPortable namedPortable : inner.nn) {
            GenericRecord namedRecord = GenericRecordBuilder.portable(namedPortableClassDefinition)
                    .setString("name", inner.nn[i].name)
                    .setInt32("myint", inner.nn[i].myint).build();
            namedRecords[i++] = namedRecord;
        }

        GenericRecord innerRecord = GenericRecordBuilder.portable(innerPortableClassDefinition)
                .setArrayOfInt8("b", inner.bb)
                .setArrayOfChar("c", inner.cc)
                .setArrayOfInt16("s", inner.ss)
                .setArrayOfInt32("i", inner.ii)
                .setArrayOfInt64("l", inner.ll)
                .setArrayOfFloat32("f", inner.ff)
                .setArrayOfFloat64("d", inner.dd)
                .setArrayOfGenericRecord("nn", namedRecords)
                .setArrayOfDecimal("bigDecimals", inner.bigDecimals)
                .setArrayOfTime("localTimes", inner.localTimes)
                .setArrayOfDate("localDates", inner.localDates)
                .setArrayOfTimestamp("localDateTimes", inner.localDateTimes)
                .setArrayOfTimestampWithTimezone("offsetDateTimes", inner.offsetDateTimes)
                .build();

        return GenericRecordBuilder.portable(mainPortableClassDefinition)
                .setInt8("b", expectedPortable.b)
                .setBoolean("bool", expectedPortable.bool)
                .setChar("c", expectedPortable.c)
                .setInt16("s", expectedPortable.s)
                .setInt32("i", expectedPortable.i)
                .setInt64("l", expectedPortable.l)
                .setFloat32("f", expectedPortable.f)
                .setFloat64("d", expectedPortable.d)
                .setString("str", expectedPortable.str)
                .setGenericRecord("p", innerRecord)
                .setDecimal("bigDecimal", expectedPortable.bigDecimal)
                .setTime("localTime", expectedPortable.localTime)
                .setDate("localDate", expectedPortable.localDate)
                .setTimestamp("localDateTime", expectedPortable.localDateTime)
                .setTimestampWithTimezone("offsetDateTime", expectedPortable.offsetDateTime)
                .build();
    }

    private BiTuple<GenericRecord, GenericRecord> getInconsistentGenericRecords() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();

        ClassDefinition inConsistentNamedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("WrongName").addIntField("myint").build();

        GenericRecord record = GenericRecordBuilder.portable(namedPortableClassDefinition)
                .setString("name", "foo")
                .setInt32("myint", 123).build();

        GenericRecord inConsistentNamedRecord = GenericRecordBuilder.portable(inConsistentNamedPortableClassDefinition)
                .setString("WrongName", "foo")
                .setInt32("myint", 123).build();

        return BiTuple.of(record, inConsistentNamedRecord);
    }

    private BiTuple<GenericRecord, GenericRecord> getInconsistentNestedGenericRecords() {
        ClassDefinition childCd = new ClassDefinitionBuilder(1, 1)
                .addIntField("a")
                .build();

        ClassDefinition inconsistentChildCd = new ClassDefinitionBuilder(1, 1)
                .addBooleanField("a")
                .build();

        ClassDefinition namedPortableClassDefinition = new ClassDefinitionBuilder(1, 2)
                .addStringField("name")
                .addPortableField("child", childCd)
                .build();

        ClassDefinition inconsistentNamedPortableClassDefinition = new ClassDefinitionBuilder(1, 2)
                .addStringField("name")
                .addPortableField("child", inconsistentChildCd)
                .build();

        GenericRecord record = GenericRecordBuilder.portable(namedPortableClassDefinition)
                .setString("name", "foo")
                .setGenericRecord("child", GenericRecordBuilder.portable(childCd)
                        .setInt32("a", 1)
                        .build()
                ).build();

        GenericRecord otherRecord = GenericRecordBuilder.portable(inconsistentNamedPortableClassDefinition)
                .setString("name", "foo")
                .setGenericRecord("child", GenericRecordBuilder.portable(inconsistentChildCd)
                        .setBoolean("a", false)
                        .build()
                ).build();

        return BiTuple.of(record, otherRecord);
    }

}
