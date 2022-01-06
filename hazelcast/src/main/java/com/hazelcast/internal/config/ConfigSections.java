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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

        private static final Map<String, String> sectionToService;
        private static final Map<String, String> serviceToSection;

        static {
            sectionToService = new HashMap<>();

            sectionToService.put(MAP.name, MapService.SERVICE_NAME);
            sectionToService.put(CACHE.name, CacheService.SERVICE_NAME);
            sectionToService.put(QUEUE.name, QueueService.SERVICE_NAME);
            sectionToService.put(LIST.name, ListService.SERVICE_NAME);
            sectionToService.put(SET.name, SetService.SERVICE_NAME);
            sectionToService.put(MULTIMAP.name, MultiMapService.SERVICE_NAME);
            sectionToService.put(REPLICATED_MAP.name, ReplicatedMapService.SERVICE_NAME);
            sectionToService.put(RINGBUFFER.name, RingbufferService.SERVICE_NAME);
            sectionToService.put(TOPIC.name, TopicService.SERVICE_NAME);
            sectionToService.put(RELIABLE_TOPIC.name, ReliableTopicService.SERVICE_NAME);
            sectionToService.put(EXECUTOR_SERVICE.name, DistributedExecutorService.SERVICE_NAME);
            sectionToService.put(DURABLE_EXECUTOR_SERVICE.name, DistributedDurableExecutorService.SERVICE_NAME);
            sectionToService.put(SCHEDULED_EXECUTOR_SERVICE.name, DistributedScheduledExecutorService.SERVICE_NAME);
            sectionToService.put(CARDINALITY_ESTIMATOR.name, CardinalityEstimatorService.SERVICE_NAME);
            sectionToService.put(PN_COUNTER.name, PNCounterService.SERVICE_NAME);
            sectionToService.put(FLAKE_ID_GENERATOR.name, FlakeIdGeneratorService.SERVICE_NAME);
            sectionToService.put(WAN_REPLICATION.name, WanReplicationService.SERVICE_NAME);

            serviceToSection = sectionToService
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        }

        public static String toServiceName(String sectionName) {
            String serviceName = sectionToService.get(sectionName);

            if (serviceName == null) {
                serviceName = "Section doesn't have translation.";
            }

            return serviceName;
        }

        public static String toSectionName(String serviceName) {
            String sectionName = serviceToSection.get(serviceName);

            if (serviceName == null) {
                sectionName = "Service doesn't have translation.";
            }

            return sectionName;
        }
    }
}
