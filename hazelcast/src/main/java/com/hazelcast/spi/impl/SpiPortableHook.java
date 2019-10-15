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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPI_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPI_PORTABLE_FACTORY_ID;

public final class SpiPortableHook implements PortableHook {

    public static final int ID = FactoryIdHelper.getFactoryId(SPI_PORTABLE_FACTORY, SPI_PORTABLE_FACTORY_ID);

    public static final int USERNAME_PWD_CRED = 1;
    public static final int COLLECTION = 2;
    public static final int ITEM_EVENT = 3;
    public static final int ENTRY_EVENT = 4;
    public static final int DISTRIBUTED_OBJECT_EVENT = 5;
    public static final int MAP_PARTITION_LOST_EVENT = 6;
    public static final int PARTITION_LOST_EVENT = 7;
    public static final int CACHE_PARTITION_LOST_EVENT = 8;
    public static final int SIMPLE_TOKEN_CRED = 9;

    @Override
    public int getFactoryId() {
        return ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            @SuppressWarnings("checkstyle:returncount")
            public Portable create(int classId) {
                switch (classId) {
                    case USERNAME_PWD_CRED:
                        return new UsernamePasswordCredentials();
                    case COLLECTION:
                        return new PortableCollection();
                    case ITEM_EVENT:
                        return new PortableItemEvent();
                    case ENTRY_EVENT:
                        return new PortableEntryEvent();
                    case DISTRIBUTED_OBJECT_EVENT:
                        return new PortableDistributedObjectEvent();
                    case MAP_PARTITION_LOST_EVENT:
                        return new PortableMapPartitionLostEvent();
                    case PARTITION_LOST_EVENT:
                        return new PortablePartitionLostEvent();
                    case CACHE_PARTITION_LOST_EVENT:
                        return new PortableCachePartitionLostEvent();
                    case SIMPLE_TOKEN_CRED:
                        return new SimpleTokenCredentials();
                    default:
                        return null;
                }
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
