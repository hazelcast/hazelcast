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
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.cache.spi.entry.StandardCacheEntryImpl;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A {@code CacheEntry} serializer compatible with the SPI interface introduced in Hibernate 4.2. For reference entries
 * the {@code CacheEntry} is serialized directly to avoid relying on too many Hibernate implementation details. Entity
 * entries (the most common type) are serialized by accessing the fields using the interface's methods, instead of via
 * Reflection like the {@link Hibernate41CacheEntrySerializer 4.1 serializer}.
 *
 * @since 3.3
 */
class Hibernate42CacheEntrySerializer
        implements StreamSerializer<CacheEntry> {

    private static final Constructor<StandardCacheEntryImpl> CACHE_ENTRY_CONSTRUCTOR;
    private static final Class<?>[] CONSTRUCTOR_ARG_TYPES = {Serializable[].class, String.class, boolean.class, Object.class};

    static {
        try {
            CACHE_ENTRY_CONSTRUCTOR = StandardCacheEntryImpl.class.getDeclaredConstructor(CONSTRUCTOR_ARG_TYPES);
            CACHE_ENTRY_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.HIBERNATE4_TYPE_HIBERNATE_CACHE_ENTRY;
    }

    @Override
    public CacheEntry read(ObjectDataInput in)
            throws IOException {

        try {
            if (in.readBoolean()) {
                return readReference(in);
            }
            return readDisassembled(in);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void write(ObjectDataOutput out, CacheEntry object)
            throws IOException {

        try {
            out.writeBoolean(object.isReferenceEntry());
            if (object.isReferenceEntry()) {
                // Reference entries are not disassembled. Instead, to be serialized, they rely entirely on
                // the entity itself being Serializable. This is not a common thing (Hibernate is currently
                // very restrictive about what can be cached by reference), so it may not be worth dealing
                // with at all. This is just a naive implementation relying on the entity's serialization.
                writeReference(out, object);
            } else {
                writeDisassembled(out, object);
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static CacheEntry readDisassembled(ObjectDataInput in)
            throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException {

        int length = in.readInt();
        Serializable[] disassembledState = new Serializable[length];
        for (int i = 0; i < length; i++) {
            disassembledState[i] = in.readObject();
        }

        String subclass = in.readUTF();
        boolean lazyPropertiesAreUnfetched = in.readBoolean();
        Object version = in.readObject();

        return CACHE_ENTRY_CONSTRUCTOR.newInstance(disassembledState, subclass, lazyPropertiesAreUnfetched, version);
    }

    private static CacheEntry readReference(ObjectDataInput in) throws IOException {
        return ((CacheEntryWrapper) in.readObject()).entry;
    }

    private static IOException rethrow(Exception e)
            throws IOException {

        if (e instanceof IOException) {
            throw (IOException) e;
        }
        throw new IOException(e);
    }

    private static void writeDisassembled(ObjectDataOutput out, CacheEntry object)
            throws IOException {

        Serializable[] disassembledState = object.getDisassembledState();
        out.writeInt(disassembledState.length);
        for (Serializable state : disassembledState) {
            out.writeObject(state);
        }

        out.writeUTF(object.getSubclass());
        out.writeBoolean(object.areLazyPropertiesUnfetched());
        out.writeObject(object.getVersion());

    }

    private static void writeReference(ObjectDataOutput out, CacheEntry object)
            throws IOException {

        out.writeObject(new CacheEntryWrapper(object));
    }

    /**
     * Wraps a CacheEntry so that serializing it will not recursively call back into this class.
     * <p/>
     * {@code CacheEntry} extends {@code Serializable}, so the entry could theoretically just be written with
     * {@code ObjectDataOutput.writeObject(Object)}. However, doing so would cause the {@code SerializationService}
     * to look up the serializer and route the entry right back here again, forming an infinite loop. This wrapper
     * type, which has no explicit mapping, should fall back on the {@code ObjectSerializer} and be serialized by
     * a standard {@code ObjectOutputStream}.
     *
     * @since 3.3
     */
    private static final class CacheEntryWrapper implements Serializable {

        private final CacheEntry entry;

        private CacheEntryWrapper(CacheEntry entry) {
            this.entry = entry;
        }
    }
}
