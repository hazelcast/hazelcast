/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Arrays;

public final class FactoryIdHelper {
    public enum Factory {
        SPI_DS("hazelcast.serialization.ds.spi", -1),
        PARTITION_DS("hazelcast.serialization.ds.partition", -2),
        CLIENT_DS("hazelcast.serialization.ds.client", -3),
        MAP_DS("hazelcast.serialization.ds.map", -4),
        QUEUE_DS("hazelcast.serialization.ds.queue", -5),
        MULTIMAP_DS("hazelcast.serialization.ds.multimap", -6),
        EXECUTOR_DS("hazelcast.serialization.ds.executor", -7),
        LOCK_DS("hazelcast.serialization.ds.lock", -8),
        TOPIC_DS("hazelcast.serialization.ds.topic", -9),
        TRANSACTION_DS("hazelcast.serialization.ds.transaction", -10),
        COLLECTION_DS("hazelcast.serialization.ds.collection", -11),
        REPLICATED_MAP_DS("hazelcast.serialization.ds.replicated_map", -12),
        CACHE_DS("hazelcast.serialization.ds.cache", -13),
        HIDENSITY_CACHE_DS("hazelcast.serialization.ds.hidensity.cache", -14),
        ENTERPRISE_CACHE_DS("hazelcast.serialization.ds.enterprise.cache", -15),
        ENTERPRISE_WAN_REPLICATION_DS("hazelcast.serialization.ds.enterprise.wan_replication", -16),
        RINGBUFFER_DS("hazelcast.serialization.ds.ringbuffer", -17),
        ENTERPRISE_MAP_DS("hazelcast.serialization.ds.enterprise.map", -18),
        HIBERNATE_DS("hazelcast.serialization.ds.hibernate"),
        WEB_DS("hazelcast.serialization.ds.web"),
        WAN_REPLICATION_DS("hazelcast.serialization.ds.wan_replication", -19),
        PREDICATE_DS("hazelcast.serialization.ds.predicate", -20),
        CARDINALITY_ESTIMATOR_DS("hazelcast.serialization.ds.cardinality_estimator", -21),
        DURABLE_EXECUTOR_DS("hazelcast.serialization.ds.durable.executor", -22),
        ENTERPRISE_HOTRESTART_CLUSTER_DS("hazelcast.serialization.ds.spi.hotrestart.cluster", -23),
        MANAGEMENT_DS("hazelcast.serialization.ds.management", -24),
        TEXT_PROTOCOLS_DS("hazelcast.serialization.ds.text.protocols", -25),
        ENTERPRISE_HOTRESTART_BACKUP_DS("hazelcast.serialization.ds.spi.hotrestart.backup", -26),
        SCHEDULED_EXECUTOR_DS("hazelcast.serialization.ds.scheduled.executor", -27),
        USER_CODE_DEPLOYMENT_DS("hazelcast.serialization.ds.user.code.deployment", -28),
        AGGREGATOR_DS("hazelcast.serialization.ds.aggregator", -29),
        PROJECTION_DS("hazelcast.serialization.ds.projection", -30),
        CONFIG_DS("hazelcast.serialization.ds.config", -31),
        ENTERPRISE_SECURITY_DS("hazelcast.serialization.ds.security", -32),
        EVENT_JOURNAL_DS("hazelcast.serialization.ds.event_journal", -33),
        FLAKE_ID_GENERATOR_DS("hazelcast.serialization.ds.flake_id_generator", -34),
        SPLIT_BRAIN_DS("hazelcast.serialization.ds.split_brain", -35),
        PN_COUNTER_DS("hazelcast.serialization.ds.pn_counter", -36),
        METRICS_DS("hazelcast.serialization.metrics", -37),
        SQL_DS("hazelcast.serialization.sql", -38),
        JSON_DS("hazelcast.serialization.json", -39),
        UTIL_COLLECTION_DS("hazelcast.serialization.util.collection", -40),
        JET_SQL_DS("hazelcast.serialization.jet.sql", -41),
        SCHEMA_DS("hazelcast.serialization.schema", -42),
        ENTERPRISE_PARTITION_DS("hazelcast.serialization.ds.enterprise.partition", -43),
        BASIC_FUNCTIONS_DS("hazelcast.serialization.lambda", -44);

        private final String prop;
        /** factory ID 0 is reserved for Cluster objects (Data, Address, Member etc)... */
        private final int defaultFactoryId;

        Factory(final String prop, final int defaultFactoryId) {
            this.prop = prop;
            this.defaultFactoryId = defaultFactoryId;
        }

        Factory(final String prop) {
            this(prop, 0);
        }

        public int getDefaultFactoryId() {
            return defaultFactoryId;
        }

        public int getFactoryId() {
            return FactoryIdHelper.getFactoryId(prop, defaultFactoryId);
        }

        /** @return the {@link toString} of the {@link Factory} indexed by {@code factoryId}, else the {@code factoryId} */
        public static String getName(final int factoryId) {
            return Arrays.stream(values()).filter(factory -> factory.getFactoryId() == factoryId).map(Factory::toString)
                    .findAny().orElseGet(() -> String.valueOf(factoryId));
        }
    }

    private FactoryIdHelper() {
    }

    public static int getFactoryId(final String prop, final int defaultId) {
        final String value = System.getProperty(prop);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (final NumberFormatException e) {
                Logger.getLogger(FactoryIdHelper.class).finest("Parameter for property prop could not be parsed", e);
            }
        }
        return defaultId;
    }
}
