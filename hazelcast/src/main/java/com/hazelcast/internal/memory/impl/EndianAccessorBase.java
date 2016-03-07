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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.ByteAccessStrategy;
import com.hazelcast.internal.memory.EndianMemoryAccessor;

import java.lang.reflect.Field;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;

/**
 * Base class for big- and little-endian implementations.
 */
abstract class EndianAccessorBase implements EndianMemoryAccessor {
    private final ByteAccessStrategy<Void> defaultByteAccessStrategy = new DefaultByteAccessStrategy(this);

    @Override
    public long objectFieldOffset(Field field) {
        return MEM.objectFieldOffset(field);
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        return MEM.arrayBaseOffset(arrayClass);
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        return MEM.arrayIndexScale(arrayClass);
    }

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long lengthBytes) {
        MEM.copyMemory(srcObj, srcOffset, destObj, destOffset, lengthBytes);
    }

    @Override
    public Object getObject(Object base, long offset) {
        return MEM.getObject(base, offset);
    }

    @Override
    public void putObject(Object base, long offset, Object x) {
        MEM.putObject(base, offset, x);
    }

    @Override
    public boolean getBoolean(Object base, long offset) {
        return MEM.getBoolean(base, offset);
    }

    @Override
    public void putBoolean(Object base, long offset, boolean x) {
        MEM.putBoolean(base, offset, x);
    }

    @Override
    public byte getByte(Object base, long offset) {
        return MEM.getByte(base, offset);
    }

    @Override
    public void putByte(Object base, long offset, byte x) {
        MEM.putByte(base, offset, x);
    }

    @Override
    public boolean getBoolean(long address) {
        return MEM.getBoolean(address);
    }

    @Override
    public void putBoolean(long address, boolean x) {
        MEM.putBoolean(address, x);
    }

    @Override
    public byte getByte(long address) {
        return MEM.getByte(address);
    }

    @Override
    public void putByte(long address, byte x) {
        MEM.putByte(address, x);
    }

    @Override
    public ByteAccessStrategy<Void> asByteAccessStrategy() {
        return defaultByteAccessStrategy;
    }
}
