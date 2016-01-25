/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.VersionedPortable;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Set;

public final class SerializationUtil {

    static final PartitioningStrategy EMPTY_PARTITIONING_STRATEGY = new PartitioningStrategy() {
        public Object getPartitionKey(Object key) {
            return null;
        }
    };

    private SerializationUtil() {
    }

    static boolean isNullData(Data data) {
        return data.dataSize() == 0 && data.getType() == SerializationConstants.CONSTANT_TYPE_NULL;
    }

    static RuntimeException handleException(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            throw (Error) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException(e);
    }

    static SerializerAdapter createSerializerAdapter(Serializer serializer, SerializationService serializationService) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer) {
            s = new StreamSerializerAdapter(serializationService, (StreamSerializer) serializer);
        } else if (serializer instanceof ByteArraySerializer) {
            s = new ByteArraySerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException(
                    "Serializer must be instance of either " + "StreamSerializer or ByteArraySerializer!");
        }
        return s;
    }

    static void getInterfaces(Class clazz, Set<Class> interfaces) {
        final Class[] classes = clazz.getInterfaces();
        if (classes.length > 0) {
            Collections.addAll(interfaces, classes);
            for (Class cl : classes) {
                getInterfaces(cl, interfaces);
            }
        }
    }

    static int indexForDefaultType(final int typeId) {
        return -typeId;
    }

    public static int getPortableVersion(Portable portable, int defaultVersion) {
        int version = defaultVersion;
        if (portable instanceof VersionedPortable) {
            VersionedPortable versionedPortable = (VersionedPortable) portable;
            version = versionedPortable.getClassVersion();
            if (version < 0) {
                throw new IllegalArgumentException("Version cannot be negative!");
            }
        }
        return version;
    }

    public static ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, SerializationService ss) {
        return new ObjectDataOutputStream(out, ss);
    }

    public static ObjectDataInputStream createObjectDataInputStream(InputStream in, SerializationService ss) {
        return new ObjectDataInputStream(in, ss);
    }

}
