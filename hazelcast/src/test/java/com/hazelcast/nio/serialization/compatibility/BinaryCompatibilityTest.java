/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class BinaryCompatibilityTest {

    private static final int NULL_OBJECT = -1;
    private static Map<String, Data> dataMap = new HashMap<String, Data>();

    //OPTIONS
    private static Object[] objects = ReferenceObjects.allTestObjects;
    private static boolean[] unsafeAllowedOpts = {true, false};
    private static ByteOrder[] byteOrders = {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
    private static byte[] versions = {1};

    @Parameterized.Parameter(0)
    public boolean allowUnsafe;
    @Parameterized.Parameter(1)
    public Object object;
    @Parameterized.Parameter(2)
    public ByteOrder byteOrder;
    @Parameterized.Parameter(3)
    public byte version;

    @BeforeClass
    public static void init() throws IOException {
        for (byte version : versions) {
            InputStream input = BinaryCompatibilityTest.class.getResourceAsStream("/" + createFileName(version));
            DataInputStream inputStream = new DataInputStream(input);
            while (input.available() != 0) {
                String objectKey = inputStream.readUTF();
                int length = inputStream.readInt();
                if (length != NULL_OBJECT) {
                    byte[] bytes = new byte[length];
                    inputStream.read(bytes);
                    dataMap.put(objectKey, new HeapData(bytes));
                }

            }
            inputStream.close();
        }
    }

    @Parameterized.Parameters(name = "allowUnsafe:{0}, {index}, isBigEndian:{2}, version:{3}")
    public static Iterable<Object[]> parameters() {


        LinkedList<Object[]> parameters = new LinkedList<Object[]>();
        for (boolean allowUnsafe : unsafeAllowedOpts) {
            for (Object object : objects) {
                for (ByteOrder byteOrder : byteOrders) {
                    for (byte version : versions) {
                        parameters.add(new Object[]{allowUnsafe, object, byteOrder, version});
                    }
                }
            }
        }
        return parameters;
    }

    private String createObjectKey() {
        return version + "-" + (object == null ? "NULL" : object.getClass().getSimpleName()) + "-" + byteOrder;
    }


    private static String createFileName(byte version) {
        return version + ".serialization.compatibility.binary";
    }

    private SerializationService createSerializationService() {
        SerializationConfig config = new SerializationConfig();
        {
            SerializerConfig serializerConfig = new SerializerConfig();
            serializerConfig.setImplementation(new CustomByteArraySerializer()).setTypeClass(CustomByteArraySerializable.class);
            config.addSerializerConfig(serializerConfig);
        }
        {
            SerializerConfig serializerConfig = new SerializerConfig();
            serializerConfig.setImplementation(new CustomStreamSerializer()).setTypeClass(CustomStreamSerializable.class);
            config.addSerializerConfig(serializerConfig);
        }
        config.setAllowUnsafe(allowUnsafe);
        config.setByteOrder(byteOrder);
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(ReferenceObjects.PORTABLE_FACTORY_ID, ReferenceObjects.INNER_PORTABLE_CLASS_ID)
                        .addIntField("i").addFloatField("f").build();

        return new DefaultSerializationServiceBuilder()
                .setConfig(config)
                .setVersion(version)
                .addPortableFactory(ReferenceObjects.PORTABLE_FACTORY_ID, new APortableFactory())
                .addDataSerializableFactory(ReferenceObjects.IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID,
                        new ADataSerializableFactory())
                .addClassDefinition(classDefinition)
                .build();
    }

    @Test
    public void readAndVerifyBinaries() throws IOException {
        String key = createObjectKey();
        SerializationService serializationService = createSerializationService();
        Object readObject = serializationService.toObject(dataMap.get(key));
        boolean equals = equals(object, readObject);
        if (!equals) {
            System.out.println(object.getClass().getSimpleName() + ": " + object + " != " + readObject);
        }
        assertTrue(equals);

    }

    @Test
    public void basicSerializeDeserialize() throws IOException {
        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(object);
        Object readObject = serializationService.toObject(data);
        assertTrue(equals(object, readObject));
    }

    public static boolean equals(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.getClass().isArray() && b.getClass().isArray()) {

            int length = Array.getLength(a);
            if (length > 0 && !a.getClass().getComponentType().equals(b.getClass().getComponentType())) {
                return false;
            }
            if (Array.getLength(b) != length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (!equals(Array.get(a, i), Array.get(b, i))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            ListIterator e1 = ((List) a).listIterator();
            ListIterator e2 = ((List) b).listIterator();
            while (e1.hasNext() && e2.hasNext()) {
                Object o1 = e1.next();
                Object o2 = e2.next();
                if (!equals(o1, o2)) {
                    return false;
                }
            }
            return !(e1.hasNext() || e2.hasNext());
        }
        return a.equals(b);
    }


}
