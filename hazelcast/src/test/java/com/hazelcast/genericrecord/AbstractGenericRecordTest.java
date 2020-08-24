/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.genericrecord;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.internal.serialization.impl.portable.InnerPortable;
import com.hazelcast.internal.serialization.impl.portable.MainPortable;
import com.hazelcast.internal.serialization.impl.portable.NamedPortable;
import com.hazelcast.internal.serialization.impl.portable.PortableTest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractGenericRecordTest extends HazelcastTestSupport {

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
    public void testPutWithoutFactory_readAsPortable() throws IOException {

        NamedPortable[] nn = new NamedPortable[2];
        nn[0] = new NamedPortable("name", 123);
        nn[1] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        MainPortable expectedMain = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        HazelcastInstance[] instances = createCluster();
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();
        ClassDefinition innerPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.INNER_PORTABLE)
                        .addByteArrayField("b")
                        .addCharArrayField("c")
                        .addShortArrayField("s")
                        .addIntArrayField("i")
                        .addLongArrayField("l")
                        .addFloatArrayField("f")
                        .addDoubleArrayField("d")
                        .addPortableArrayField("nn", namedPortableClassDefinition).build();
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
                        .addUTFField("str")
                        .addPortableField("p", innerPortableClassDefinition)
                        .build();

        GenericRecord namedRecord = GenericRecord.Builder.portable(namedPortableClassDefinition)
                .writeUTF("name", nn[0].name)
                .writeInt("myint", nn[0].myint).build();
        GenericRecord[] namedRecords = new GenericRecord[2];
        namedRecords[0] = namedRecord;
        namedRecords[1] = namedRecord;

        GenericRecord innerRecord = GenericRecord.Builder.portable(innerPortableClassDefinition)
                .writeByteArray("b", inner.bb)
                .writeCharArray("c", inner.cc)
                .writeShortArray("s", inner.ss)
                .writeIntArray("i", inner.ii)
                .writeLongArray("l", inner.ll)
                .writeFloatArray("f", inner.ff)
                .writeDoubleArray("d", inner.dd)
                .writeGenericRecordArray("nn", namedRecords).build();

        GenericRecord expected = GenericRecord.Builder.portable(mainPortableClassDefinition)
                .writeByte("b", expectedMain.b)
                .writeBoolean("bool", expectedMain.bool)
                .writeChar("c", expectedMain.c)
                .writeShort("s", expectedMain.s)
                .writeInt("i", expectedMain.i)
                .writeLong("l", expectedMain.l)
                .writeFloat("f", expectedMain.f)
                .writeDouble("d", expectedMain.d)
                .writeUTF("str", expectedMain.str)
                .writeGenericRecord("p", innerRecord)
                .build();

        assertEquals(expectedMain.c, expected.readChar("c"));
        assertEquals(expectedMain.f, expected.readFloat("f"), 0.1);

        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        clusterMap.put(1, expected);

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");

        MainPortable actual = (MainPortable) map.get(1);

        assertEquals(expectedMain, actual);
    }

    @Test
    public void testPutGenericRecordBack() throws IOException {

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

        assertEquals(expected.name, actualRecord.readUTF("name"));
        assertEquals(expected.myint, actualRecord.readInt("myint"));


        //read from the instance with serialization config
        NamedPortable actualPortable = (NamedPortable) map.get(2);
        assertEquals(expected, actualPortable);
    }

    @Test
    public void testReadReturnsGenericRecord() throws IOException {

        HazelcastInstance[] instances = createCluster();

        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        NamedPortable expected = new NamedPortable("foo", 900);
        map.put(1, expected);

        IMap<Object, Object> clusterMap = instances[0].getMap("test");
        GenericRecord actual = (GenericRecord) clusterMap.get(1);

        assertTrue(actual.hasField("name"));
        assertTrue(actual.hasField("myint"));

        assertEquals(expected.name, actual.readUTF("name"));
        assertEquals(expected.myint, actual.readInt("myint"));
    }

    @Test
    public void testEntryProcessorReturnsGenericRecord() throws IOException {

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
                    .writeUTF("name", "bar")
                    .writeInt("myint", 4).build();

            entry.setValue(modifiedGenericRecord);

            return genericRecord.readInt("myint");
        });
        assertEquals(expected.myint, returnValue);

        NamedPortable actualPortable = (NamedPortable) map.get(key);
        assertEquals("bar", actualPortable.name);
        assertEquals(4, actualPortable.myint);
    }

    @Test
    public void testCloneWithGenericBuilderOnEntryProcessor() throws IOException {

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
                    .writeInt("myint", 4).build();

            entry.setValue(modifiedGenericRecord);

            return genericRecord.readInt("myint");
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
            return genericRecord.readInt("myint");
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
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();

        ClassDefinition inConsistentNamedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("WrongName").addIntField("myint").build();


        GenericRecord namedRecord = GenericRecord.Builder.portable(namedPortableClassDefinition)
                .writeUTF("name", "foo")
                .writeInt("myint", 123).build();


        GenericRecord inConsistentNamedRecord = GenericRecord.Builder.portable(inConsistentNamedPortableClassDefinition)
                .writeUTF("WrongName", "foo")
                .writeInt("myint", 123).build();


        HazelcastInstance instance = createAccessorInstance(serializationConfig);
        IMap<Object, Object> map = instance.getMap("test");
        map.put(1, namedRecord);

        map.put(2, inConsistentNamedRecord);
    }

}
