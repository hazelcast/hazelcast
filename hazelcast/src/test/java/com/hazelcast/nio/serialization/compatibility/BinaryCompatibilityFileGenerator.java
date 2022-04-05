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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;

/**
 * This class is used for generating the binary file to be committed at the beginning of
 * introducing a new serialization service. Change version field and run this class once.
 * Then move the created files to resources directory.
 *
 * mv *binary src/test/resources/
 */
final class BinaryCompatibilityFileGenerator {

    // DON'T FORGET TO CHANGE VERSION ACCORDINGLY
    public static final byte VERSION = 1;
    private static final int NULL_OBJECT = -1;

    private BinaryCompatibilityFileGenerator() {
    }

    public static void main(String[] args) throws IOException {
        Object[] objects = ReferenceObjects.allTestObjects;

        OutputStream out = new FileOutputStream(createFileName());
        DataOutputStream outputStream = new DataOutputStream(out);
        ByteOrder[] byteOrders = {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
        for (Object object : objects) {
            for (ByteOrder byteOrder : byteOrders) {
                generateBinaryFile(outputStream, object, byteOrder);
            }
        }
        outputStream.close();
    }

    private static String createObjectKey(Object object, ByteOrder byteOrder) {
        return VERSION + "-" + (object == null ? "NULL" : object.getClass().getSimpleName()) + "-" + byteOrder;
    }

    private static String createFileName() {
        return VERSION + ".serialization.compatibility.binary";
    }

    @SuppressWarnings("checkstyle:avoidnestedblocks")
    private static SerializationService createSerializationService(ByteOrder byteOrder) {
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
        config.setByteOrder(byteOrder);
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(ReferenceObjects.PORTABLE_FACTORY_ID, ReferenceObjects.INNER_PORTABLE_CLASS_ID)
                        .addIntField("i").addFloatField("f").build();

        return new DefaultSerializationServiceBuilder()
                .setConfig(config)
                .setVersion(VERSION)
                .addPortableFactory(ReferenceObjects.PORTABLE_FACTORY_ID, new APortableFactory())
                .addDataSerializableFactory(ReferenceObjects.IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID,
                        new ADataSerializableFactory())
                .addClassDefinition(classDefinition)
                .build();
    }

    private static void generateBinaryFile(DataOutputStream outputStream,
                                           Object object, ByteOrder byteOrder) throws IOException {
        SerializationService serializationService = createSerializationService(byteOrder);
        Data data = serializationService.toData(object);
        outputStream.writeUTF(createObjectKey(object, byteOrder));
        if (data == null) {
            outputStream.writeInt(NULL_OBJECT);
            return;
        }
        byte[] bytes = data.toByteArray();
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }
}
