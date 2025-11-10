/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;

public class CustomByteArraySerializer implements ByteArraySerializer<CustomByteArraySerializable> {

    @Override
    public int getTypeId() {
        return ReferenceObjects.CUSTOM_BYTE_ARRAY_SERIALIZABLE_ID;
    }

    @Override
    public byte[] write(CustomByteArraySerializable object) {
        byte[] bytes = new byte[10];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        wrap.putInt(object.i);
        wrap.putFloat(object.f);
        return bytes;
    }

    @Override
    public CustomByteArraySerializable read(byte[] buffer) {
        ByteBuffer wrap = ByteBuffer.wrap(buffer);
        return new CustomByteArraySerializable(wrap.getInt(), wrap.getFloat());
    }

    @Override
    public void destroy() {
    }
}
