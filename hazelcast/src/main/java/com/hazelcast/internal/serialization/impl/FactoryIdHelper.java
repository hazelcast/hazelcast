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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.logging.Logger;

public final class FactoryIdHelper {

    public static final String SPI_DS_FACTORY = "hazelcast.serialization.ds.spi";
    public static final int SPI_DS_FACTORY_ID = -1;

    public static final String PARTITION_DS_FACTORY = "hazelcast.serialization.ds.partition";
    public static final int PARTITION_DS_FACTORY_ID = -2;

    public static final String CLIENT_DS_FACTORY = "hazelcast.serialization.ds.client";
    public static final int CLIENT_DS_FACTORY_ID = -3;

    public static final String MAP_DS_FACTORY = "hazelcast.serialization.ds.map";
    public static final int MAP_DS_FACTORY_ID = -4;

    public static final String QUEUE_DS_FACTORY = "hazelcast.serialization.ds.queue";
    public static final int QUEUE_DS_FACTORY_ID = -5;

    public static final String MULTIMAP_DS_FACTORY = "hazelcast.serialization.ds.multimap";
    public static final int MULTIMAP_DS_FACTORY_ID = -6;

    public static final String EXECUTOR_DS_FACTORY = "hazelcast.serialization.ds.executor";
    public static final int EXECUTOR_DS_FACTORY_ID = -7;

    public static final String LOCK_DS_FACTORY = "hazelcast.serialization.ds.lock";
    public static final int LOCK_DS_FACTORY_ID = -8;

    public static final String TOPIC_DS_FACTORY = "hazelcast.serialization.ds.topic";
    public static final int TOPIC_DS_FACTORY_ID = -9;

    public static final String TRANSACTION_DS_FACTORY = "hazelcast.serialization.ds.transaction";
    public static final int TRANSACTION_DS_FACTORY_ID = -10;

    public static final String COLLECTION_DS_FACTORY = "hazelcast.serialization.ds.collection";
    public static final int COLLECTION_DS_FACTORY_ID = -11;

    public static final String REPLICATED_MAP_DS_FACTORY = "hazelcast.serialization.ds.replicated_map";
    public static final int REPLICATED_MAP_DS_FACTORY_ID = -12;

    public static final String CACHE_DS_FACTORY = "hazelcast.serialization.ds.cache";
    public static final int CACHE_DS_FACTORY_ID = -13;

    public static final String HIDENSITY_CACHE_DS_FACTORY = "hazelcast.serialization.ds.hidensity.cache";
    public static final int HIDENSITY_CACHE_DS_FACTORY_ID = -14;

    public static final String ENTERPRISE_CACHE_DS_FACTORY = "hazelcast.serialization.ds.enterprise.cache";
    public static final int ENTERPRISE_CACHE_DS_FACTORY_ID = -15;

    public static final String ENTERPRISE_WAN_REPLICATION_DS_FACTORY = "hazelcast.serialization.ds.enterprise.wan_replication";
    public static final int ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID = -16;

    public static final String RINGBUFFER_DS_FACTORY = "hazelcast.serialization.ds.ringbuffer";
    public static final int RINGBUFFER_DS_FACTORY_ID = -17;

    public static final String ENTERPRISE_MAP_DS_FACTORY = "hazelcast.serialization.ds.enterprise.map";
    public static final int ENTERPRISE_MAP_DS_FACTORY_ID = -18;

    public static final String HIBERNATE_DS_FACTORY = "hazelcast.serialization.ds.hibernate";
    public static final String WEB_DS_FACTORY = "hazelcast.serialization.ds.web";

    public static final String WAN_REPLICATION_DS_FACTORY = "hazelcast.serialization.ds.wan_replication";
    public static final int WAN_REPLICATION_DS_FACTORY_ID = -19;

    public static final String PREDICATE_DS_FACTORY = "hazelcast.serialization.ds.predicate";
    public static final int PREDICATE_DS_FACTORY_ID = -20;

    public static final String CARDINALITY_ESTIMATOR_DS_FACTORY = "hazelcast.serialization.ds.cardinality_estimator";
    public static final int CARDINALITY_ESTIMATOR_DS_FACTORY_ID = -21;

    public static final String DURABLE_EXECUTOR_DS_FACTORY = "hazelcast.serialization.ds.durable.executor";
    public static final int DURABLE_EXECUTOR_DS_FACTORY_ID = -22;

    public static final String ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY = "hazelcast.serialization.ds.spi.hotrestart.cluster";
    public static final int ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY_ID = -23;

    public static final String MANAGEMENT_DS_FACTORY = "hazelcast.serialization.ds.management";
    public static final int MANAGEMENT_DS_FACTORY_ID = -24;

    public static final String TEXT_PROTOCOLS_DS_FACTORY = "hazelcast.serialization.ds.text.protocols";
    public static final int TEXT_PROTOCOLS_DS_FACTORY_ID = -25;

    public static final String ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY = "hazelcast.serialization.ds.spi.hotrestart.backup";
    public static final int ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY_ID = -26;

    public static final String SCHEDULED_EXECUTOR_DS_FACTORY = "hazelcast.serialization.ds.scheduled.executor";
    public static final int SCHEDULED_EXECUTOR_DS_FACTORY_ID = -27;

    public static final String USER_CODE_DEPLOYMENT_DS_FACTORY = "hazelcast.serialization.ds.user.code.deployment";
    public static final int USER_CODE_DEPLOYMENT_DS_FACTORY_ID = -28;

    public static final String AGGREGATOR_DS_FACTORY = "hazelcast.serialization.ds.aggregator";
    public static final int AGGREGATOR_DS_FACTORY_ID = -29;

    public static final String PROJECTION_DS_FACTORY = "hazelcast.serialization.ds.projection";
    public static final int PROJECTION_DS_FACTORY_ID = -30;

    public static final String CONFIG_DS_FACTORY = "hazelcast.serialization.ds.config";
    public static final int CONFIG_DS_FACTORY_ID = -31;

    public static final String ENTERPRISE_SECURITY_DS_FACTORY = "hazelcast.serialization.ds.security";
    public static final int ENTERPRISE_SECURITY_DS_FACTORY_ID = -32;

    public static final String EVENT_JOURNAL_DS_FACTORY = "hazelcast.serialization.ds.event_journal";
    public static final int EVENT_JOURNAL_DS_FACTORY_ID = -33;

    public static final String FLAKE_ID_GENERATOR_DS_FACTORY = "hazelcast.serialization.ds.flake_id_generator";
    public static final int FLAKE_ID_GENERATOR_DS_FACTORY_ID = -34;

    public static final String SPLIT_BRAIN_DS_FACTORY = "hazelcast.serialization.ds.split_brain";
    public static final int SPLIT_BRAIN_DS_FACTORY_ID = -35;

    public static final String PN_COUNTER_DS_FACTORY = "hazelcast.serialization.ds.pn_counter";
    public static final int PN_COUNTER_DS_FACTORY_ID = -36;

    public static final String METRICS_DS_FACTORY = "hazelcast.serialization.metrics";
    public static final int METRICS_DS_FACTORY_ID = -37;

    public static final String SQL_DS_FACTORY = "hazelcast.serialization.sql";
    public static final int SQL_DS_FACTORY_ID = -38;

    public static final String JSON_DS_FACTORY = "hazelcast.serialization.json";
    public static final int JSON_DS_FACTORY_ID = -39;

    public static final String UTIL_COLLECTION_DS_FACTORY = "hazelcast.serialization.util.collection";
    public static final int UTIL_COLLECTION_DS_FACTORY_ID = -40;

    public static final String JET_SQL_DS_FACTORY = "hazelcast.serialization.jet.sql";
    public static final int JET_SQL_DS_FACTORY_ID = -41;

    public static final String SCHEMA_DS_FACTORY = "hazelcast.serialization.schema";
    public static final int SCHEMA_DS_FACTORY_ID = -42;

    public static final String ENTERPRISE_PARTITION_DS_FACTORY = "hazelcast.serialization.ds.enterprise.partition";
    public static final int ENTERPRISE_PARTITION_DS_FACTORY_ID = -43;

    public static final String BASIC_FUNCTIONS_DS_FACTORY = "hazelcast.serialization.lambda";
    public static final int BASIC_FUNCTIONS_DS_FACTORY_ID = -44;

    // factory ID 0 is reserved for Cluster objects (Data, Address, Member etc)...

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
