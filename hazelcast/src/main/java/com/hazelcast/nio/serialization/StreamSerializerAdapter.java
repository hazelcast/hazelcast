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


import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;

final class StreamSerializerAdapter implements SerializerAdapter {

    private final SerializationServiceImpl service;
    private final StreamSerializer serializer;

    public StreamSerializerAdapter(SerializationServiceImpl service, StreamSerializer serializer) {
        this.service = service;
        this.serializer = serializer;
    }

    @SuppressWarnings("unchecked")
    public void write(ObjectDataOutput out, Object object) throws IOException {
        serializer.write(out, object);
    }

    public Object read(ObjectDataInput in) throws IOException {
        return serializer.read(in);
    }

    @SuppressWarnings("unchecked")
    public byte[] write(Object object) throws IOException {
        final BufferObjectDataOutput out = service.pop();
        byte[] bytes;
        try {
            serializer.write(out, object);
            bytes = out.toByteArray();
        } finally {
            service.push(out);
        }
        return bytes;
    }

    public Object read(Data data) throws IOException {
        final BufferObjectDataInput in = service.createObjectDataInput(data);
        try {
            return serializer.read(in);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    public int getTypeId() {
        return serializer.getTypeId();
    }

    public void destroy() {
        serializer.destroy();
    }

    @Override
    public Serializer getImpl() {
        return serializer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializerAdapter{");
        sb.append("serializer=").append(serializer);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamSerializerAdapter that = (StreamSerializerAdapter) o;

        if (serializer != null ? !serializer.equals(that.serializer) : that.serializer != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return serializer != null ? serializer.hashCode() : 0;
    }
}
