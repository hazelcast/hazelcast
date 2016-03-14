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

import com.hazelcast.internal.memory.EndianMemoryAccessor;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;

import java.lang.reflect.Field;

/**
 * Base class for big- and little-endian implementations.
 */
abstract class EndianAccessorBase implements EndianMemoryAccessor {

    @Override
    public long objectFieldOffset(Field field) {
        return GlobalMemoryAccessorRegistry.MEM.objectFieldOffset(field);
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        return GlobalMemoryAccessorRegistry.MEM.arrayBaseOffset(arrayClass);
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        return GlobalMemoryAccessorRegistry.MEM.arrayIndexScale(arrayClass);
    }

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long lengthBytes) {
        GlobalMemoryAccessorRegistry.MEM.copyMemory(srcObj, srcOffset, destObj, destOffset, lengthBytes);
    }

    @Override
    public Object getObject(Object base, long offset) {
        return GlobalMemoryAccessorRegistry.MEM.getObject(base, offset);
    }

    @Override
    public void putObject(Object base, long offset, Object x) {
        GlobalMemoryAccessorRegistry.MEM.putObject(base, offset, x);
    }

    @Override
    public boolean getBoolean(Object base, long offset) {
        return GlobalMemoryAccessorRegistry.MEM.getBoolean(base, offset);
    }

    @Override
    public void putBoolean(Object base, long offset, boolean x) {
        GlobalMemoryAccessorRegistry.MEM.putBoolean(base, offset, x);
    }

    @Override
    public byte getByte(Object base, long offset) {
        return GlobalMemoryAccessorRegistry.MEM.getByte(base, offset);
    }

    @Override
    public void putByte(Object base, long offset, byte x) {
        GlobalMemoryAccessorRegistry.MEM.putByte(base, offset, x);
    }

    @Override
    public boolean getBoolean(long address) {
        return GlobalMemoryAccessorRegistry.MEM.getBoolean(address);
    }

    @Override
    public void putBoolean(long address, boolean x) {
        GlobalMemoryAccessorRegistry.MEM.putBoolean(address, x);
    }

    @Override
    public byte getByte(long address) {
        return GlobalMemoryAccessorRegistry.MEM.getByte(address);
    }

    @Override
    public void putByte(long address, byte x) {
        GlobalMemoryAccessorRegistry.MEM.putByte(address, x);
    }
}
