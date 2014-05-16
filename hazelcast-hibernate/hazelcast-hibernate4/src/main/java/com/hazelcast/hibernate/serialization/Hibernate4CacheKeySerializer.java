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
import org.hibernate.cache.spi.CacheKey;
import org.hibernate.type.Type;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * The actual CacheKey serializer implementation
 */
class Hibernate4CacheKeySerializer
        implements StreamSerializer<CacheKey> {

    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final long KEY_OFFSET;
    private static final long TYPE_OFFSET;
    private static final long ENTITY_OR_ROLE_NAME_OFFSET;
    private static final long TENANT_ID_OFFSET;
    private static final long HASH_CODE_OFFSET;

    static {
        try {
            Class<CacheKey> cacheKeyClass = CacheKey.class;
            Field key = cacheKeyClass.getDeclaredField("key");
            KEY_OFFSET = UNSAFE.objectFieldOffset(key);

            Field type = cacheKeyClass.getDeclaredField("type");
            TYPE_OFFSET = UNSAFE.objectFieldOffset(type);

            Field entityOrRoleName = cacheKeyClass.getDeclaredField("entityOrRoleName");
            ENTITY_OR_ROLE_NAME_OFFSET = UNSAFE.objectFieldOffset(entityOrRoleName);

            Field tenantId = cacheKeyClass.getDeclaredField("tenantId");
            TENANT_ID_OFFSET = UNSAFE.objectFieldOffset(tenantId);

            Field hashCode = cacheKeyClass.getDeclaredField("hashCode");
            HASH_CODE_OFFSET = UNSAFE.objectFieldOffset(hashCode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(ObjectDataOutput out, CacheKey object)
            throws IOException {

        try {
            Object key = UNSAFE.getObject(object, KEY_OFFSET);
            Type type = (Type) UNSAFE.getObject(object, TYPE_OFFSET);
            String entityOrRoleName = (String) UNSAFE.getObject(object, ENTITY_OR_ROLE_NAME_OFFSET);
            String tenantId = (String) UNSAFE.getObject(object, TENANT_ID_OFFSET);
            int hashCode = UNSAFE.getInt(object, HASH_CODE_OFFSET);

            out.writeObject(key);
            out.writeObject(type);
            out.writeUTF(entityOrRoleName);
            out.writeUTF(tenantId);
            out.writeInt(hashCode);

        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    @Override
    public CacheKey read(ObjectDataInput in)
            throws IOException {

        try {
            Object key = in.readObject();
            Type type = in.readObject();
            String entityOrRoleName = in.readUTF();
            String tenantId = in.readUTF();
            int hashCode = in.readInt();

            CacheKey cacheKey = (CacheKey) UNSAFE.allocateInstance(CacheKey.class);
            UNSAFE.putObjectVolatile(cacheKey, KEY_OFFSET, key);
            UNSAFE.putObjectVolatile(cacheKey, TYPE_OFFSET, type);
            UNSAFE.putObjectVolatile(cacheKey, ENTITY_OR_ROLE_NAME_OFFSET, entityOrRoleName);
            UNSAFE.putObjectVolatile(cacheKey, TENANT_ID_OFFSET, tenantId);
            UNSAFE.putIntVolatile(cacheKey, HASH_CODE_OFFSET, hashCode);
            return cacheKey;

        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.HIBERNATE3_TYPE_HIBERNATE_CACHE_KEY;
    }

    @Override
    public void destroy() {
    }
}
