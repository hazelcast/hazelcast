/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * This InputStream implementation intercepts {@link #resolveObject(Object)} to
 * analyse if the read object is of type
 * {@link com.hazelcast.nio.serialization.InterceptingObjectProxy} (written by
 * {@link com.hazelcast.nio.serialization.InterceptingObjectOutputStream} which
 * encapsulates a Hazelcast DataSerializable object.
 */
public class InterceptingObjectInputStream
        extends ObjectInputStream {

    private final SerializationService serializationService;

    public InterceptingObjectInputStream(InputStream in, ObjectDataInput dataInput)
            throws IOException {

        super(in);
        enableResolveObject(true);
        this.serializationService = SerializationServiceAccessor.getSerializationService(dataInput);
    }

    @Override
    protected Object resolveObject(Object obj)
            throws IOException {

        if (obj instanceof InterceptingObjectProxy) {
            InterceptingObjectProxy proxy = (InterceptingObjectProxy) obj;
            BufferObjectDataInput objectDataInput = serializationService.createObjectDataInput(proxy.getData());
            try {
                return objectDataInput.readObject();
            } finally {
                IOUtil.closeResource(objectDataInput);
            }
        }
        return obj;
    }
}
