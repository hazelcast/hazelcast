/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY_ID;

/**
 * DataSerializerHook for com.hazelcast.config classes
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class ConfigDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CONFIG_DS_FACTORY, CONFIG_DS_FACTORY_ID);

    public static final int WAN_REPLICATION_CONFIG = 0;
    public static final int WAN_CONSUMER_CONFIG = 1;
    public static final int WAN_PUBLISHER_CONFIG = 2;
    public static final int NEAR_CACHE_CONFIG = 3;
    public static final int NEAR_CACHE_PRELOADER_CONFIG = 4;

    private static final int LEN = NEAR_CACHE_PRELOADER_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[WAN_REPLICATION_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanReplicationConfig();
            }
        };
        constructors[WAN_CONSUMER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanConsumerConfig();
            }
        };
        constructors[WAN_PUBLISHER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanPublisherConfig();
            }
        };
        constructors[NEAR_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCacheConfig();
            }
        };
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCachePreloaderConfig();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
