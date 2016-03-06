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

/**
 * The {@link ByteArraySerializerStreamSerializerAdapter} adapts a {@link ByteArraySerializer} to behave like a
 * {@link StreamSerializer}.
 */
final class ByteArraySerializerStreamSerializerAdapter implements StreamSerializer {

    protected final ByteArraySerializer serializer;

    public ByteArraySerializerStreamSerializerAdapter(ByteArraySerializer serializer) {
        this.serializer = checkNotNull(serializer, "serializer can't be null");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(ObjectDataOutput out, Object object) throws IOException {
        byte[] bytes = serializer.write(object);
        out.writeByteArray(bytes);
    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        if (bytes == null) {
            return null;
        }
        return serializer.read(bytes);
    }

    @Override
    public int getTypeId() {
        return serializer.getTypeId();
    }

    @Override
    public void destroy() {
        serializer.destroy();
    }

    public Serializer getImpl() {
        return serializer;
    }

    @Override
    public String toString() {
        return "StreamSerializer{serializer=" + serializer + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ByteArraySerializerStreamSerializerAdapter that = (ByteArraySerializerStreamSerializerAdapter) o;

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
