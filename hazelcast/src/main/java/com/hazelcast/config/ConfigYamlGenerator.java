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

package com.hazelcast.config;

import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.ConfigXmlGenerator.classNameOrImplClass;

public class ConfigYamlGenerator {

    private static final int INDENT = 2;

    String generate(Config config) {
        Map<String, Object> document = new LinkedHashMap<>();
        Map<String, Object> root = new LinkedHashMap<>();
        document.put("hazelcast", root);

        root.put("cluster-name", config.getClusterName());

        replicatedMapConfigYamlGenerator(root, config);
        ringbufferYamlGenerator(root, config);
        topicYamlGenerator(root, config);
        reliableTopicYamlGenerator(root, config);
        executorYamlGenerator(root, config);
        durableExecutorYamlGenerator(root, config);
        scheduledExecutorYamlGenerator(root, config);
        cardinalityEstimatorYamlGenerator(root, config);
        flakeIdGeneratorYamlGenerator(root, config);
        pnCounterYamlGenerator(root, config);


        DumpSettings dumpSettings = DumpSettings.builder()
                .setDefaultFlowStyle(FlowStyle.BLOCK)
                .setIndent(INDENT)
                .build();
        Dump dump = new Dump(dumpSettings);
        return dump.dumpToString(document);
    }

//    static void xxxGenerator(Map<String, Object> parent, Config config) {
//        if (config.xxx.isEmpty()) {
//            return;
//        }
//
//        Map<String, Object> child = new LinkedHashMap<>();
//        for (xxx subConfigAsObject : config.xxx.values()) {
//            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();
//
//
//            child.put(subConfigAsObject.getName(), subConfigAsMap);
//        }
//
//        parent.put(xxx, child);
//    }

        static void replicatedMapConfigYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReplicatedMapConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReplicatedMapConfig subConfigAsObject : config.getReplicatedMapConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "in-memory-format", subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "async-fillup", subConfigAsObject.isAsyncFillup());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "entry-listeners", getEntryListenerConfigsAsList(subConfigAsObject.getListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("replicatedmap", child);
    }

    static void ringbufferYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getRingbufferConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (RingbufferConfig subConfigAsObject : config.getRingbufferConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "time-to-live-seconds", subConfigAsObject.getTimeToLiveSeconds());
            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "in-memory-format", subConfigAsObject.getInMemoryFormat().name());
            addNonNullToMap(subConfigAsMap, "ringbuffer-store", getRingbufferStoreConfigAsMap(subConfigAsObject.getRingbufferStoreConfig()));
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("ringbuffer", child);
    }

    static void topicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (TopicConfig subConfigAsObject : config.getTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "global-ordering-enabled", subConfigAsObject.isGlobalOrderingEnabled());
            addNonNullToMap(subConfigAsMap, "message-listeners", getListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));
            addNonNullToMap(subConfigAsMap, "multi-threading-enabled", subConfigAsObject.isMultiThreadingEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("topic", child);
    }

    static void reliableTopicYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getReliableTopicConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ReliableTopicConfig subConfigAsObject : config.getReliableTopicConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "read-batch-size", subConfigAsObject.getReadBatchSize());
            addNonNullToMap(subConfigAsMap, "topic-overload-policy", subConfigAsObject.getTopicOverloadPolicy().name());
            addNonNullToMap(subConfigAsMap, "message-listeners", getListenerConfigsAsList(subConfigAsObject.getMessageListenerConfigs()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("reliable-topic", child);
    }

    static void executorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ExecutorConfig subConfigAsObject : config.getExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());
            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "queue-capacity", subConfigAsObject.getQueueCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("executor-service", child);
    }

    static void durableExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getDurableExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (DurableExecutorConfig subConfigAsObject : config.getDurableExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability", subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("durable-executor-service", child);
    }

    static void scheduledExecutorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getScheduledExecutorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (ScheduledExecutorConfig subConfigAsObject : config.getScheduledExecutorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "pool-size", subConfigAsObject.getPoolSize());
            addNonNullToMap(subConfigAsMap, "durability", subConfigAsObject.getDurability());
            addNonNullToMap(subConfigAsMap, "capacity", subConfigAsObject.getCapacity());
            addNonNullToMap(subConfigAsMap, "capacity-policy", subConfigAsObject.getCapacityPolicy().name());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("scheduled-executor-service", child);
    }

    static void cardinalityEstimatorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getCardinalityEstimatorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (CardinalityEstimatorConfig subConfigAsObject : config.getCardinalityEstimatorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "backup-count", subConfigAsObject.getBackupCount());
            addNonNullToMap(subConfigAsMap, "async-backup-count", subConfigAsObject.getAsyncBackupCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "merge-policy", getMergePolicyConfigAsMap(subConfigAsObject.getMergePolicyConfig()));

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("cardinality-estimator", child);
    }

    static void flakeIdGeneratorYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getFlakeIdGeneratorConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (FlakeIdGeneratorConfig subConfigAsObject : config.getFlakeIdGeneratorConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "prefetch-count", subConfigAsObject.getPrefetchCount());
            addNonNullToMap(subConfigAsMap, "prefetch-validity-millis", subConfigAsObject.getPrefetchValidityMillis());
            addNonNullToMap(subConfigAsMap, "epoch-start", subConfigAsObject.getEpochStart());
            addNonNullToMap(subConfigAsMap, "node-id-offset", subConfigAsObject.getNodeIdOffset());
            addNonNullToMap(subConfigAsMap, "bits-sequence", subConfigAsObject.getBitsSequence());
            addNonNullToMap(subConfigAsMap, "bits-node-id", subConfigAsObject.getBitsNodeId());
            addNonNullToMap(subConfigAsMap, "allowed-future-millis", subConfigAsObject.getAllowedFutureMillis());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("flake-id-generator", child);
    }

    static void pnCounterYamlGenerator(Map<String, Object> parent, Config config) {
        if (config.getPNCounterConfigs().isEmpty()) {
            return;
        }

        Map<String, Object> child = new LinkedHashMap<>();
        for (PNCounterConfig subConfigAsObject : config.getPNCounterConfigs().values()) {
            Map<String, Object> subConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(subConfigAsMap, "replica-count", subConfigAsObject.getReplicaCount());
            addNonNullToMap(subConfigAsMap, "split-brain-protection-ref", subConfigAsObject.getSplitBrainProtectionName());
            addNonNullToMap(subConfigAsMap, "statistics-enabled", subConfigAsObject.isStatisticsEnabled());

            child.put(subConfigAsObject.getName(), subConfigAsMap);
        }

        parent.put("pn-counter", child);
    }

    private static List<Map<String, Object>> getEntryListenerConfigsAsList(List<ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            Map<String, Object> listenerConfigAsMap = new LinkedHashMap<>();

            addNonNullToMap(listenerConfigAsMap, "class-name", classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
            addNonNullToMap(listenerConfigAsMap, "include-value", listenerConfig.isIncludeValue());
            addNonNullToMap(listenerConfigAsMap, "local", listenerConfig.isLocal());

            addNonNullToList(listenerConfigsAsList, listenerConfigAsMap);
        }

        return listenerConfigsAsList;
    }

    private static Map<String, Object> getPropertiesAsMap(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        Map<String, Object> propertiesAsMap = new LinkedHashMap<>();

        for (Object key : properties.keySet()) {
            addNonNullToMap(propertiesAsMap, key.toString(), properties.getProperty(key.toString()));
        }

        return propertiesAsMap;
    }

    private static Map<String, Object> getRingbufferStoreConfigAsMap(RingbufferStoreConfig ringbufferStoreConfig) {
        if (ringbufferStoreConfig == null) {
            return null;
        }

        Map<String, Object> getRingbufferStoreConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(getRingbufferStoreConfigAsMap, "enabled", ringbufferStoreConfig.isEnabled());
        addNonNullToMap(getRingbufferStoreConfigAsMap, "class-name", classNameOrImplClass(ringbufferStoreConfig.getClassName(), ringbufferStoreConfig.getStoreImplementation()));
        addNonNullToMap(getRingbufferStoreConfigAsMap, "factory-class-name", classNameOrImplClass(ringbufferStoreConfig.getFactoryClassName(), ringbufferStoreConfig.getFactoryImplementation()));
        addNonNullToMap(getRingbufferStoreConfigAsMap, "properties", getPropertiesAsMap(ringbufferStoreConfig.getProperties()));

        return getRingbufferStoreConfigAsMap;
    }

    private static List<String> getListenerConfigsAsList(List<ListenerConfig> listenerConfigs) {
        if (listenerConfigs == null || listenerConfigs.isEmpty()) {
            return null;
        }

        List<String> listenerConfigsAsList = new LinkedList<>();

        for (ListenerConfig listenerConfig : listenerConfigs) {
            addNonNullToList(listenerConfigsAsList, classNameOrImplClass(listenerConfig.getClassName(), listenerConfig.getImplementation()));
        }

        return listenerConfigsAsList;
    }

    private static Map<String, Object> getMergePolicyConfigAsMap(MergePolicyConfig mergePolicyConfig) {
        if (mergePolicyConfig == null) {
            return null;
        }

        Map<String, Object> mergePolicyConfigAsMap = new LinkedHashMap<>();

        addNonNullToMap(mergePolicyConfigAsMap, "class-name", mergePolicyConfig.getPolicy());
        addNonNullToMap(mergePolicyConfigAsMap, "batch-size", mergePolicyConfig.getBatchSize());

        return mergePolicyConfigAsMap;
    }

    private static <E> void addNonNullToList(List<E> list, E element) {
        if (element != null) {
            list.add(element);
        }
    }

    private static <K, V> void addNonNullToMap(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
