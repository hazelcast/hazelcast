/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_CONFIG_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_CONFIG_DS_FACTORY_ID;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.config} package. This is private API.
 */
@PrivateApi
public final class JetConfigDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link JobConfig} class.
     */
    public static final int JOB_CONFIG = 0;

    /**
     * Serialization ID of the {@link EdgeConfig} class.
     */
    public static final int EDGE_CONFIG = 1;

    /**
     * Serialization ID of the {@link ResourceConfig} class.
     */
    public static final int RESOURCE_CONFIG = 2;

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_CONFIG_DS_FACTORY, JET_CONFIG_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case JOB_CONFIG:
                    return new JobConfig();
                case EDGE_CONFIG:
                    return new EdgeConfig();
                case RESOURCE_CONFIG:
                    return new ResourceConfig();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
