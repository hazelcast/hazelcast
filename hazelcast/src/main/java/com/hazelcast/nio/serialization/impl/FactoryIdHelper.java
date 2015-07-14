/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.impl;

import com.hazelcast.logging.Logger;

public final class FactoryIdHelper {

    public static final String SPI_DS_FACTORY = "hazelcast.serialization.ds.spi";
    public static final int SPI_DS_FACTORY_ID = -1;

    public static final String PARTITION_DS_FACTORY = "hazelcast.serialization.ds.partition";
    public static final int PARTITION_DS_FACTORY_ID = -2;

    public static final String CLIENT_DS_FACTORY = "hazelcast.serialization.ds.client";
    public static final int CLIENT_DS_FACTORY_ID = -3;

    public static final String MAP_DS_FACTORY = "hazelcast.serialization.ds.map";
    public static final int MAP_DS_FACTORY_ID = -10;

    public static final String QUEUE_DS_FACTORY = "hazelcast.serialization.ds.queue";
    public static final int QUEUE_DS_FACTORY_ID = -11;

    public static final String MULTIMAP_DS_FACTORY = "hazelcast.serialization.ds.multimap";
    public static final int MULTIMAP_DS_FACTORY_ID = -12;

    public static final String EXECUTOR_DS_FACTORY = "hazelcast.serialization.ds.executor";
    public static final int EXECUTOR_DS_FACTORY_ID = -13;

    public static final String CDL_DS_FACTORY = "hazelcast.serialization.ds.cdl";
    public static final int CDL_DS_FACTORY_ID = -14;

    public static final String LOCK_DS_FACTORY = "hazelcast.serialization.ds.lock";
    public static final int LOCK_DS_FACTORY_ID = -15;

    public static final String SEMAPHORE_DS_FACTORY = "hazelcast.serialization.ds.semaphore";
    public static final int SEMAPHORE_DS_FACTORY_ID = -16;

    public static final String ATOMIC_LONG_DS_FACTORY = "hazelcast.serialization.ds.atomic_long";
    public static final int ATOMIC_LONG_DS_FACTORY_ID = -17;

    public static final String TOPIC_DS_FACTORY = "hazelcast.serialization.ds.topic";
    public static final int TOPIC_DS_FACTORY_ID = -18;

    public static final String COLLECTION_DS_FACTORY = "hazelcast.serialization.ds.collection";
    public static final int COLLECTION_DS_FACTORY_ID = -20;

    public static final String ATOMIC_REFERENCE_DS_FACTORY = "hazelcast.serialization.ds.atomic_reference";
    public static final int ATOMIC_REFERENCE_DS_FACTORY_ID = -21;

    public static final String REPLICATED_MAP_DS_FACTORY = "hazelcast.serialization.ds.replicated_map";
    public static final int REPLICATED_MAP_DS_FACTORY_ID = -22;

    public static final String MAP_REDUCE_DS_FACTORY = "hazelcast.serialization.ds.map_reduce";
    public static final int MAP_REDUCE_DS_FACTORY_ID = -23;

    public static final String AGGREGATIONS_DS_FACTORY = "hazelcast.serialization.ds.aggregations";
    public static final int AGGREGATIONS_DS_FACTORY_ID = -24;

    public static final String CACHE_DS_FACTORY = "hazelcast.serialization.ds.cache";
    public static final int CACHE_DS_FACTORY_ID = -25;

    public static final String HIDENSITY_CACHE_DS_FACTORY = "hazelcast.serialization.ds.hidensity.cache";
    public static final int HIDENSITY_CACHE_DS_FACTORY_ID = -26;

    public static final String ENTERPRISE_CACHE_DS_FACTORY = "hazelcast.serialization.ds.enterprise.cache";
    public static final int ENTERPRISE_CACHE_DS_FACTORY_ID = -27;

    public static final String ENTERPRISE_WAN_REPLICATION_DS_FACTORY = "hazelcast.serialization.ds.enterprise.wan_replication";
    public static final int ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID = -28;

    public static final String RINGBUFFER_DS_FACTORY = "hazelcast.serialization.ds.ringbuffer";
    public static final int RINGBUFFER_DS_FACTORY_ID = -29;

    public static final String HIBERNATE_DS_FACTORY = "hazelcast.serialization.ds.hibernate";
    public static final String WEB_DS_FACTORY = "hazelcast.serialization.ds.web";

    // =========================== portables =============================================

    public static final String SPI_PORTABLE_FACTORY = "hazelcast.serialization.portable.spi";
    public static final int SPI_PORTABLE_FACTORY_ID = -1;

    public static final String PARTITION_PORTABLE_FACTORY = "hazelcast.serialization.portable.partition";
    public static final int PARTITION_PORTABLE_FACTORY_ID = -2;

    public static final String CLIENT_PORTABLE_FACTORY = "hazelcast.serialization.portable.client";
    public static final int CLIENT_PORTABLE_FACTORY_ID = -3;

    public static final String MAP_PORTABLE_FACTORY = "hazelcast.serialization.portable.map";
    public static final int MAP_PORTABLE_FACTORY_ID = -10;

    public static final String ENTERPRISE_MAP_PORTABLE_FACTORY = "hazelcast.serialization.portable.enterprise.map";

    public static final String QUEUE_PORTABLE_FACTORY = "hazelcast.serialization.portable.queue";
    public static final int QUEUE_PORTABLE_FACTORY_ID = -11;

    public static final String MULTIMAP_PORTABLE_FACTORY = "hazelcast.serialization.portable.multimap";
    public static final int MULTIMAP_PORTABLE_FACTORY_ID = -12;

    public static final String EXECUTOR_PORTABLE_FACTORY = "hazelcast.serialization.portable.executor";
    public static final int EXECUTOR_PORTABLE_FACTORY_ID = -13;

    public static final String CDL_PORTABLE_FACTORY = "hazelcast.serialization.portable.cdl";
    public static final int CDL_PORTABLE_FACTORY_ID = -14;

    public static final String LOCK_PORTABLE_FACTORY = "hazelcast.serialization.portable.lock";
    public static final int LOCK_PORTABLE_FACTORY_ID = -15;

    public static final String SEMAPHORE_PORTABLE_FACTORY = "hazelcast.serialization.portable.semaphore";
    public static final int SEMAPHORE_PORTABLE_FACTORY_ID = -16;

    public static final String ATOMIC_LONG_PORTABLE_FACTORY = "hazelcast.serialization.portable.atomic_long";
    public static final int ATOMIC_LONG_PORTABLE_FACTORY_ID = -17;

    public static final String TOPIC_PORTABLE_FACTORY = "hazelcast.serialization.portable.topic";
    public static final int TOPIC_PORTABLE_FACTORY_ID = -18;

    public static final String CLIENT_TXN_PORTABLE_FACTORY = "hazelcast.serialization.portable.client.txn";
    public static final int CLIENT_TXN_PORTABLE_FACTORY_ID = -19;

    public static final String COLLECTION_PORTABLE_FACTORY = "hazelcast.serialization.portable.collection";
    public static final int COLLECTION_PORTABLE_FACTORY_ID = -20;

    public static final String ATOMIC_REFERENCE_PORTABLE_FACTORY = "hazelcast.serialization.portable.atomic_reference";
    public static final int ATOMIC_REFERENCE_PORTABLE_FACTORY_ID = -21;

    public static final String REPLICATED_PORTABLE_FACTORY = "hazelcast.serialization.portable.replicated_map";
    public static final int REPLICATED_PORTABLE_FACTORY_ID = -22;

    public static final String MAP_REDUCE_PORTABLE_FACTORY = "hazelcast.serialization.portable.map_reduce";
    public static final int MAP_REDUCE_PORTABLE_FACTORY_ID = -23;

    public static final String CACHE_PORTABLE_FACTORY = "hazelcast.serialization.portable.cache";
    public static final int CACHE_PORTABLE_FACTORY_ID = -24;

    public static final String RINGBUFFER_PORTABLE_FACTORY = "hazelcast.serialization.portable.ringbuffer";
    public static final int RINGBUFFER_PORTABLE_FACTORY_ID = -29;

    // factory id 0 is reserved for Cluster objects (Data, Address, Member etc)...

    private FactoryIdHelper() {
    }


    public static int getFactoryId(String prop, int defaultId) {
        final String value = System.getProperty(prop);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                Logger.getLogger(FactoryIdHelper.class).finest("Parameter for property prop could not be parsed", e);
            }
        }
        return defaultId;
    }
}
