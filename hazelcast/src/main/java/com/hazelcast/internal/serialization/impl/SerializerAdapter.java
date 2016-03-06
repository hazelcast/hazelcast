/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

final class SerializerAdapter {

    private final StreamSerializer serializer;
    private final int typeId;
    private final Serializer impl;

    public SerializerAdapter(Serializer serializer) {
        checkNotNull(serializer, "serializer can't be null");

        this.impl = serializer;
        if (serializer instanceof StreamSerializer) {
            this.serializer = (StreamSerializer) serializer;
        } else if (serializer instanceof ByteArraySerializer) {
            this.serializer = new ByteArraySerializerStreamSerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException("Unrecognized serializer:" + serializer);
        }

        this.typeId = serializer.getTypeId();
    }

    @SuppressWarnings("unchecked")
    public void write(ObjectDataOutput out, Object object) throws IOException {
        serializer.write(out, object);
    }

    public Object read(ObjectDataInput in) throws IOException {
        return serializer.read(in);
    }

    public int getTypeId() {
        return typeId;
    }

    public void destroy() {
        serializer.destroy();
    }

    public Serializer getImpl() {
        return impl;
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

        SerializerAdapter that = (SerializerAdapter) o;

        if (!serializer.equals(that.serializer)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return serializer.hashCode();
    }
}
