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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import static com.hazelcast.internal.nio.IOUtil.newObjectInputStream;

/**
 * Wrapper class used for deserialization using a custom class loader
 */
public final class CustomClassLoadedObject {

    protected final Object object;

    private CustomClassLoadedObject(Object object) {
        this.object = object;
    }

    public static void write(ObjectDataOutput out, Serializable object) throws IOException {
        out.writeObject(new CustomClassLoadedObject(object));
    }

    public static <T> T read(ObjectDataInput input) throws IOException {
        //noinspection unchecked
        return (T) ((CustomClassLoadedObject) input.readObject()).object;
    }

    public static <T> T deserializeWithCustomClassLoader(
            SerializationService serializationService, ClassLoader cl, Data data
    ) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(cl);
        try {
            return serializationService.toObject(data);
        } catch (HazelcastSerializationException e) {
            throw ExceptionUtil.handleSerializedLambdaCce(e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    public static final class Hook implements SerializerHook<CustomClassLoadedObject> {

        @Override
        public Class<CustomClassLoadedObject> getSerializationType() {
            return CustomClassLoadedObject.class;
        }

        @Override
        public Serializer createSerializer() {
            return new Serializer();
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class Serializer implements StreamSerializer<CustomClassLoadedObject> {

        @Override
        // explicit cast to OutputStream and intentionally omitting to close ObjectOutputStream
        @SuppressFBWarnings({"BC_UNCONFIRMED_CAST", "OS_OPEN_STREAM"})
        public void write(ObjectDataOutput out, CustomClassLoadedObject object) throws IOException {
            boolean isJavaSerialized = !(object.object instanceof DataSerializable);
            out.writeBoolean(isJavaSerialized);
            if (isJavaSerialized) {
                final ObjectOutputStream objectOutputStream = new ObjectOutputStream((OutputStream) out);
                objectOutputStream.writeObject(object.object);
                // Force flush if not yet written due to internal behavior if pos < 1024
                objectOutputStream.flush();
            } else {
                out.writeObject(object.object);
            }
        }

        @Override
        // explicit cast to InputStream and intentionally omitting to close ObjectInputStream
        @SuppressFBWarnings({"BC_UNCONFIRMED_CAST", "OS_OPEN_STREAM"})
        public CustomClassLoadedObject read(com.hazelcast.nio.ObjectDataInput in) throws IOException {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            boolean isJavaSerialized = in.readBoolean();
            Object object;
            if (isJavaSerialized) {
                ObjectInputStream objectInputStream = newObjectInputStream(cl, null, (InputStream) in);
                try {
                    object = objectInputStream.readObject();
                } catch (ClassNotFoundException e) {
                    throw new HazelcastSerializationException(e);
                }
            } else {
                object = in.readObject();
            }
            return new CustomClassLoadedObject(object);
        }

        @Override
        public int getTypeId() {
            return SerializerHookConstants.CUSTOM_CLASS_LOADED_OBJECT;
        }
    }
}
