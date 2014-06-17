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

package com.hazelcast.hibernate.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.hibernate.cache.spi.entry.CacheEntry;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * The actual CacheKey serializer implementation
 */
class Hibernate41CacheEntrySerializer
        implements StreamSerializer<CacheEntry> {

    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final long DISASSEMBLED_STATE_OFFSET;
    private static final long SUBCLASS_OFFSET;
    private static final long LAZY_PROPERTIES_ARE_UNFETCHED;
    private static final long VERSION_OFFSET;

    private static final Class<?>[] CONSTRUCTOR_ARG_TYPES = {Serializable[].class, String.class, boolean.class, Object.class};
    private static final Constructor<CacheEntry> CACHE_ENTRY_CONSTRUCTOR;

    static {
        try {
            Class<CacheEntry> cacheEntryClass = CacheEntry.class;
            Field disassembledState = cacheEntryClass.getDeclaredField("disassembledState");
            DISASSEMBLED_STATE_OFFSET = UNSAFE.objectFieldOffset(disassembledState);

            Field subclass = cacheEntryClass.getDeclaredField("subclass");
            SUBCLASS_OFFSET = UNSAFE.objectFieldOffset(subclass);

            Field lazyPropertiesAreUnfetched = cacheEntryClass.getDeclaredField("lazyPropertiesAreUnfetched");
            LAZY_PROPERTIES_ARE_UNFETCHED = UNSAFE.objectFieldOffset(lazyPropertiesAreUnfetched);

            Field version = cacheEntryClass.getDeclaredField("version");
            VERSION_OFFSET = UNSAFE.objectFieldOffset(version);

            CACHE_ENTRY_CONSTRUCTOR = cacheEntryClass.getDeclaredConstructor(CONSTRUCTOR_ARG_TYPES);
            CACHE_ENTRY_CONSTRUCTOR.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(ObjectDataOutput out, CacheEntry object)
            throws IOException {

        try {
            Serializable[] disassembledState = (Serializable[]) UNSAFE.getObject(object, DISASSEMBLED_STATE_OFFSET);
            String subclass = (String) UNSAFE.getObject(object, SUBCLASS_OFFSET);
            boolean lazyPropertiesAreUnfetched = UNSAFE.getBoolean(object, LAZY_PROPERTIES_ARE_UNFETCHED);
            Object version = UNSAFE.getObject(object, VERSION_OFFSET);

            out.writeInt(disassembledState.length);
            for (Serializable state : disassembledState) {
                out.writeObject(state);
            }
            out.writeUTF(subclass);
            out.writeBoolean(lazyPropertiesAreUnfetched);
            out.writeObject(version);

        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    @Override
    public CacheEntry read(ObjectDataInput in)
            throws IOException {

        try {
            int length = in.readInt();
            Serializable[] disassembledState = new Serializable[length];
            for (int i = 0; i < length; i++) {
                disassembledState[i] = in.readObject();
            }
            String subclass = in.readUTF();
            boolean lazyPropertiesAreUnfetched = in.readBoolean();
            Object version = in.readObject();

            return CACHE_ENTRY_CONSTRUCTOR.newInstance(disassembledState, subclass, lazyPropertiesAreUnfetched, version);

        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.HIBERNATE4_TYPE_HIBERNATE_CACHE_ENTRY;
    }

    @Override
    public void destroy() {
    }
}
