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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import static com.hazelcast.nio.serialization.compatibility.ReferenceObjects.IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID;
import static com.hazelcast.nio.serialization.compatibility.ReferenceObjects.INNER_PORTABLE_CLASS_ID;
import static com.hazelcast.nio.serialization.compatibility.ReferenceObjects.PORTABLE_FACTORY_ID;
import static com.hazelcast.test.HazelcastTestSupport.assumeConfiguredByteOrder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class BinaryCompatibilityTest {

    private static final int NULL_OBJECT = -1;
    private static Map<String, Data> dataMap = new HashMap<String, Data>();

    // OPTIONS
    private static Object[] objects = ReferenceObjects.allTestObjects;
    private static boolean[] unsafeAllowedOpts = {true, false};
    private static ByteOrder[] byteOrders = {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
    private static byte[] versions = {1};

    @Parameters(name = "allowUnsafe:{0}, {index}, isBigEndian:{2}, version:{3}")
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

    @Parameter
    public boolean allowUnsafe;
    @Parameter(1)
    public Object object;
    @Parameter(2)
    public ByteOrder byteOrder;
    @Parameter(3)
    public byte version;

    @BeforeClass
    public static void init() throws IOException {
        for (byte version : versions) {
            InputStream input = BinaryCompatibilityTest.class.getResourceAsStream("/" + createFileName(version));
            if (input == null) {
                fail("Could not locate file " + createFileName(version) + ". Follow the instructions in "
                        + "BinaryCompatibilityFileGenerator to generate the file.");
            }
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

    @Test
    public void readAndVerifyBinaries() {
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
    public void basicSerializeDeserialize() {
        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(object);
        Object readObject = serializationService.toObject(data);
        assertTrue(equals(object, readObject));
    }

    private String createObjectKey() {
        return version + "-" + (object == null ? "NULL" : object.getClass().getSimpleName()) + "-" + byteOrder;
    }

    private SerializationService createSerializationService() {
        SerializerConfig customByteArraySerializerConfig = new SerializerConfig()
                .setImplementation(new CustomByteArraySerializer())
                .setTypeClass(CustomByteArraySerializable.class);

        SerializerConfig customStreamSerializerConfig = new SerializerConfig()
                .setImplementation(new CustomStreamSerializer())
                .setTypeClass(CustomStreamSerializable.class);

        SerializationConfig config = new SerializationConfig()
                .addSerializerConfig(customByteArraySerializerConfig)
                .addSerializerConfig(customStreamSerializerConfig)
                .setAllowUnsafe(allowUnsafe)
                .setByteOrder(byteOrder);

        ClassDefinition classDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, INNER_PORTABLE_CLASS_ID)
                .addIntField("i")
                .addFloatField("f")
                .build();

        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setVersion(version)
                .addPortableFactory(PORTABLE_FACTORY_ID, new APortableFactory())
                .addDataSerializableFactory(IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID, new ADataSerializableFactory())
                .setConfig(config)
                .addClassDefinition(classDefinition)
                .build();

        assumeConfiguredByteOrder(serializationService, byteOrder);

        return serializationService;
    }

    private static boolean equals(Object a, Object b) {
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
        if (a instanceof Collection && b instanceof Collection) {
            Iterator e1 = ((Collection) a).iterator();
            Iterator e2 = ((Collection) b).iterator();
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

    private static String createFileName(byte version) {
        return version + ".serialization.compatibility.binary";
    }
}
