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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.compatibility.serialization.impl.CompatibilitySerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * The {@code java.util.concurrent.LinkedTransferQueue} serializer
 */
@SuppressFBWarnings("REC_CATCH_EXCEPTION")
public class LinkedTransferQueueStreamSerializer<E> implements StreamSerializer<Object> {

    private final Constructor<?> defaultConstructor;
    private final Method addMethod;
    private final Method sizeMethod;

    public LinkedTransferQueueStreamSerializer() {
        try {
            Class<?> ltqClass = Class.forName("java.util.concurrent.LinkedTransferQueue");
            this.defaultConstructor = ltqClass.getDeclaredConstructor();
            this.addMethod = ltqClass.getMethod("add", Object.class);
            this.sizeMethod = ltqClass.getMethod("size");
        } catch (Exception e) {
            throw new HazelcastException("Cannot find LinkedTransferQueue. Are you running with JDK8?", e);
        }
    }

    @Override
    public int getTypeId() {
        return CompatibilitySerializationConstants.JAVA_DEFAULT_TYPE_LINKED_TRANSFER_QUEUE;
    }

    @Override
    public void destroy() {

    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        try {
            int size = in.readInt();
            Object collection = defaultConstructor.newInstance();
            for (int i = 0; i < size; i++) {
                Object item = in.readObject();
                addMethod.invoke(collection, item);
            }
            return collection;
        } catch (Exception e) {
            throw new HazelcastException("Cannot instantiate LinkedTransferQueue. Are you running with JDK8?", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(ObjectDataOutput out, Object linkedTransferQueue) throws IOException {
        try {
            int size = (Integer) sizeMethod.invoke(linkedTransferQueue);
            out.writeInt(size);
            for (Object o : (Iterable<Object>) linkedTransferQueue) {
                out.writeObject(o);
            }
        } catch (Exception e) {
            throw new HazelcastException("Cannot find methods on LinkedTransferQueue. Are you running with JDK8?");
        }
    }
}
