/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.ByteAccessStrategy;

import java.nio.ByteBuffer;

public final class ByteBufferAccessStrategy implements ByteAccessStrategy<ByteBuffer> {

    public static final ByteBufferAccessStrategy INSTANCE = new ByteBufferAccessStrategy();

    private ByteBufferAccessStrategy() {
    }

    @Override
    public byte getByte(ByteBuffer resource, long offset) {
        return resource.get((int) offset);
    }

    @Override
    public void putByte(ByteBuffer resource, long offset, byte value) {
        resource.put((int) offset, value);
    }
}
