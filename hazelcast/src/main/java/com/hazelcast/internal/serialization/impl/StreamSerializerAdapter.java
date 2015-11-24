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


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

class StreamSerializerAdapter implements SerializerAdapter {

    protected final SerializationService service;
    protected final StreamSerializer serializer;

    public StreamSerializerAdapter(SerializationService service, StreamSerializer serializer) {
        this.service = service;
        this.serializer = serializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(ObjectDataOutput out, Object object) throws IOException {
        serializer.write(out, object);
    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        return serializer.read(in);
    }

    @Override
    public int getTypeId() {
        return serializer.getTypeId();
    }

    @Override
    public void destroy() {
        serializer.destroy();
    }

    @Override
    public Serializer getImpl() {
        return serializer;
    }

    @Override
    public String toString() {
        return "StreamSerializerAdapter{serializer=" + serializer + '}';
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
