/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.wan.impl.WanReplicationService;

/**
 * Configuration sections for the members shared by XML and YAML based
 * configurations
 */
public enum ConfigSections {
    HAZELCAST("hazelcast", false),
    INSTANCE_NAME("instance-name", false),
    IMPORT("import", true),
    CONFIG_REPLACERS("config-replacers", false),
    CLUSTER_NAME("cluster-name", false),
    LICENSE_KEY("license-key", false),
    MANAGEMENT_CENTER("management-center", false),
    PROPERTIES("properties", false),
    WAN_REPLICATION("wan-replication", true),
    NETWORK("network", false),
    PARTITION_GROUP("partition-group", false),
    EXECUTOR_SERVICE("executor-service", true),
    DURABLE_EXECUTOR_SERVICE("durable-executor-service", true),
    SCHEDULED_EXECUTOR_SERVICE("scheduled-executor-service", true),
    QUEUE("queue", true),
    MAP("map", true),
    CACHE("cache", true),
    MULTIMAP("multimap", true),
    REPLICATED_MAP("replicatedmap", true),
    LIST("list", true),
    SET("set", true),
    TOPIC("topic", true),
    RELIABLE_TOPIC("reliable-topic", true),
    RINGBUFFER("ringbuffer", true),
    LISTENERS("listeners", false),
    SERIALIZATION("serialization", false),
    SECURITY("security", false),
    MEMBER_ATTRIBUTES("member-attributes", false),
    NATIVE_MEMORY("native-memory", false),
    SPLIT_BRAIN_PROTECTION("split-brain-protection", true),
    LITE_MEMBER("lite-member", false),
    HOT_RESTART_PERSISTENCE("hot-restart-persistence", false),
    PERSISTENCE("persistence", false),
    USER_CODE_DEPLOYMENT("user-code-deployment", false),
    CARDINALITY_ESTIMATOR("cardinality-estimator", true),
    RELIABLE_ID_GENERATOR("reliable-id-generator", true),
    FLAKE_ID_GENERATOR("flake-id-generator", true),
    CRDT_REPLICATION("crdt-replication", false),
    PN_COUNTER("pn-counter", true),
    ADVANCED_NETWORK("advanced-network", false),
    CP_SUBSYSTEM("cp-subsystem", false),
    METRICS("metrics", false),
    AUDITLOG("auditlog", false),
    INSTANCE_TRACKING("instance-tracking", false),
    SQL("sql", false),
    JET("jet", false),
    LOCAL_DEVICE("local-device", true);

    final boolean multipleOccurrence;
    private final String name;

    ConfigSections(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (ConfigSections element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }

    public static class Translate {
        public static String toServiceName(String sectionName) {
            String serviceName;

            if (sectionName.equals(MAP.name)) {
                serviceName = MapService.SERVICE_NAME;
            } else if (sectionName.equals(CACHE.name)) {
                serviceName = CacheService.SERVICE_NAME;
            } else if (sectionName.equals(QUEUE.name)) {
                serviceName = QueueService.SERVICE_NAME;
            } else if (sectionName.equals(LIST.name)) {
                serviceName = ListService.SERVICE_NAME;
            } else if (sectionName.equals(SET.name)) {
                serviceName = SetService.SERVICE_NAME;
            } else if (sectionName.equals(MULTIMAP.name)) {
                serviceName = MultiMapService.SERVICE_NAME;
            } else if (sectionName.equals(REPLICATED_MAP.name)) {
                serviceName = ReplicatedMapService.SERVICE_NAME;
            } else if (sectionName.equals(RINGBUFFER.name)) {
                serviceName = RingbufferService.SERVICE_NAME;
            } else if (sectionName.equals(TOPIC.name)) {
                serviceName = TopicService.SERVICE_NAME;
            } else if (sectionName.equals(RELIABLE_TOPIC.name)) {
                serviceName = ReliableTopicService.SERVICE_NAME;
            } else if (sectionName.equals(EXECUTOR_SERVICE.name)) {
                serviceName = DistributedExecutorService.SERVICE_NAME;
            } else if (sectionName.equals(DURABLE_EXECUTOR_SERVICE.name)) {
                serviceName = DistributedDurableExecutorService.SERVICE_NAME;
            } else if (sectionName.equals(SCHEDULED_EXECUTOR_SERVICE.name)) {
                serviceName = DistributedScheduledExecutorService.SERVICE_NAME;
            } else if (sectionName.equals(CARDINALITY_ESTIMATOR.name)) {
                serviceName = CardinalityEstimatorService.SERVICE_NAME;
            } else if (sectionName.equals(PN_COUNTER.name)) {
                serviceName = PNCounterService.SERVICE_NAME;
            } else if (sectionName.equals(FLAKE_ID_GENERATOR.name)) {
                serviceName = FlakeIdGeneratorService.SERVICE_NAME;
            } else if (sectionName.equals(WAN_REPLICATION.name)) {
                serviceName = WanReplicationService.SERVICE_NAME;
            } else {
                serviceName = "Section doesn't have translation.";
            }

            return serviceName;
        }

        public static String toSectionName(String serviceName) {
            String sectionName;

            if (serviceName.equals(MapService.SERVICE_NAME)) {
                sectionName = MAP.name;
            } else if (serviceName.equals(CacheService.SERVICE_NAME)) {
                sectionName = CACHE.name;
            } else if (serviceName.equals(QueueService.SERVICE_NAME)) {
                sectionName = QUEUE.name;
            } else if (serviceName.equals(ListService.SERVICE_NAME)) {
                sectionName = LIST.name;
            } else if (serviceName.equals(SetService.SERVICE_NAME)) {
                sectionName = SET.name;
            } else if (serviceName.equals(MultiMapService.SERVICE_NAME)) {
                sectionName = MULTIMAP.name;
            } else if (serviceName.equals(ReplicatedMapService.SERVICE_NAME)) {
                sectionName = REPLICATED_MAP.name;
            } else if (serviceName.equals(RingbufferService.SERVICE_NAME)) {
                sectionName = RINGBUFFER.name;
            } else if (serviceName.equals(TopicService.SERVICE_NAME)) {
                sectionName = TOPIC.name;
            } else if (serviceName.equals(ReliableTopicService.SERVICE_NAME)) {
                sectionName = RELIABLE_TOPIC.name;
            } else if (serviceName.equals(DistributedExecutorService.SERVICE_NAME)) {
                sectionName = EXECUTOR_SERVICE.name;
            } else if (serviceName.equals(DistributedDurableExecutorService.SERVICE_NAME)) {
                sectionName = DURABLE_EXECUTOR_SERVICE.name;
            } else if (serviceName.equals(DistributedScheduledExecutorService.SERVICE_NAME)) {
                sectionName = SCHEDULED_EXECUTOR_SERVICE.name;
            } else if (serviceName.equals(CardinalityEstimatorService.SERVICE_NAME)) {
                sectionName = CARDINALITY_ESTIMATOR.name;
            } else if (serviceName.equals(PNCounterService.SERVICE_NAME)) {
                sectionName = PN_COUNTER.name;
            } else if (serviceName.equals(FlakeIdGeneratorService.SERVICE_NAME)) {
                sectionName = FLAKE_ID_GENERATOR.name;
            } else if (serviceName.equals(WanReplicationService.SERVICE_NAME)) {
                sectionName = WAN_REPLICATION.name;
            } else {
                sectionName = "Service doesn't have translation.";
            }

            return sectionName;
        }
    }
}
