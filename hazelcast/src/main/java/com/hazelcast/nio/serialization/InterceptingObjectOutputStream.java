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

import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class InterceptingObjectOutputStream
        extends ObjectOutputStream {
    private final SerializationServiceImpl serializationService;

    public InterceptingObjectOutputStream(OutputStream out, ObjectDataOutput dataOutput)
            throws IOException {

        super(out);
        enableReplaceObject(true);
        this.serializationService = (SerializationServiceImpl) dataOutput.getSerializationService();
    }

    @Override
    protected Object replaceObject(Object obj)
            throws IOException {

        if (obj instanceof DataSerializable) {
            BufferObjectDataOutput output = serializationService.pop();
            try {
                return serializeDataSerializable((DataSerializable) obj, output);
            } finally {
                serializationService.push(output);
            }
        } else {
            return obj;
        }
    }

    private InterceptingObjectProxy serializeDataSerializable(DataSerializable obj, BufferObjectDataOutput output)
            throws IOException {

        output.writeObject(obj);
        byte[] buffer = output.getBuffer();
        int position = output.position();

        byte[] data = new byte[position];
        System.arraycopy(buffer, 0, data, 0, position);
        return new InterceptingObjectProxy(data);
    }
}
