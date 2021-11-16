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

import com.hazelcast.internal.util.XmlUtil;
import com.hazelcast.spi.annotation.PrivateApi;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.config.ConfigSections.CACHE;
import static com.hazelcast.internal.config.ConfigSections.CARDINALITY_ESTIMATOR;
import static com.hazelcast.internal.config.ConfigSections.DURABLE_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.FLAKE_ID_GENERATOR;
import static com.hazelcast.internal.config.ConfigSections.LIST;
import static com.hazelcast.internal.config.ConfigSections.MAP;
import static com.hazelcast.internal.config.ConfigSections.MULTIMAP;
import static com.hazelcast.internal.config.ConfigSections.PN_COUNTER;
import static com.hazelcast.internal.config.ConfigSections.QUEUE;
import static com.hazelcast.internal.config.ConfigSections.RELIABLE_TOPIC;
import static com.hazelcast.internal.config.ConfigSections.REPLICATED_MAP;
import static com.hazelcast.internal.config.ConfigSections.RINGBUFFER;
import static com.hazelcast.internal.config.ConfigSections.SCHEDULED_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.SET;
import static com.hazelcast.internal.config.ConfigSections.TOPIC;
import static com.hazelcast.internal.config.ConfigSections.WAN_REPLICATION;

@PrivateApi
public final class DynamicConfigGenerator {

    private DynamicConfigGenerator() {
        //not called
    }

    public static String wanReplicationConfigGenerator(WanReplicationConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                WAN_REPLICATION.getName(),
                Config::addWanReplicationConfig,
                ConfigXmlGenerator::wanReplicationXmlGenerator,
                ConfigYamlGenerator::wanReplicationYamlGenerator
        );
    }

    public static String mapConfigGenerator(MapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                MAP.getName(),
                Config::addMapConfig,
                ConfigXmlGenerator::mapXmlGenerator,
                ConfigYamlGenerator::mapYamlGenerator
        );
    }

    public static String cacheConfigGenerator(CacheSimpleConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                CACHE.getName(),
                Config::addCacheConfig,
                ConfigXmlGenerator::cacheXmlGenerator,
                ConfigYamlGenerator::cacheYamlGenerator
        );
    }

    public static String queueConfigGenerator(QueueConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                QUEUE.getName(),
                Config::addQueueConfig,
                ConfigXmlGenerator::queueXmlGenerator,
                ConfigYamlGenerator::queueYamlGenerator
        );
    }

    public static String listConfigGenerator(ListConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                LIST.getName(),
                Config::addListConfig,
                ConfigXmlGenerator::listXmlGenerator,
                ConfigYamlGenerator::listYamlGenerator
        );
    }

    public static String setConfigGenerator(SetConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                SET.getName(),
                Config::addSetConfig,
                ConfigXmlGenerator::setXmlGenerator,
                ConfigYamlGenerator::setYamlGenerator
        );
    }

    public static String multiMapConfigGenerator(MultiMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                MULTIMAP.getName(),
                Config::addMultiMapConfig,
                ConfigXmlGenerator::multiMapXmlGenerator,
                ConfigYamlGenerator::multiMapYamlGenerator
        );
    }

    public static String replicatedMapConfigGenerator(ReplicatedMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                REPLICATED_MAP.getName(),
                Config::addReplicatedMapConfig,
                ConfigXmlGenerator::replicatedMapXmlGenerator,
                ConfigYamlGenerator::replicatedMapYamlGenerator
        );
    }

    public static String ringbufferConfigGenerator(RingbufferConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                RINGBUFFER.getName(),
                Config::addRingBufferConfig,
                ConfigXmlGenerator::ringbufferXmlGenerator,
                ConfigYamlGenerator::ringbufferYamlGenerator
        );
    }

    public static String topicConfigGenerator(TopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                TOPIC.getName(),
                Config::addTopicConfig,
                ConfigXmlGenerator::topicXmlGenerator,
                ConfigYamlGenerator::topicYamlGenerator
        );
    }

    public static String reliableTopicConfigGenerator(ReliableTopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                RELIABLE_TOPIC.getName(),
                Config::addReliableTopicConfig,
                ConfigXmlGenerator::reliableTopicXmlGenerator,
                ConfigYamlGenerator::reliableTopicYamlGenerator
        );
    }

    public static String executorConfigGenerator(ExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                EXECUTOR_SERVICE.getName(),
                Config::addExecutorConfig,
                ConfigXmlGenerator::executorXmlGenerator,
                ConfigYamlGenerator::executorYamlGenerator
        );
    }

    public static String durableExecutorConfigGenerator(DurableExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                DURABLE_EXECUTOR_SERVICE.getName(),
                Config::addDurableExecutorConfig,
                ConfigXmlGenerator::durableExecutorXmlGenerator,
                ConfigYamlGenerator::durableExecutorYamlGenerator
        );
    }

    public static String scheduledExecutorConfigGenerator(ScheduledExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                SCHEDULED_EXECUTOR_SERVICE.getName(),
                Config::addScheduledExecutorConfig,
                ConfigXmlGenerator::scheduledExecutorXmlGenerator,
                ConfigYamlGenerator::scheduledExecutorYamlGenerator
        );
    }

    public static String cardinalityEstimatorConfigGenerator(CardinalityEstimatorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                CARDINALITY_ESTIMATOR.getName(),
                Config::addCardinalityEstimatorConfig,
                ConfigXmlGenerator::cardinalityEstimatorXmlGenerator,
                ConfigYamlGenerator::cardinalityEstimatorYamlGenerator
        );
    }

    public static String flakeIdGeneratorConfigGenerator(FlakeIdGeneratorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                FLAKE_ID_GENERATOR.getName(),
                Config::addFlakeIdGeneratorConfig,
                ConfigXmlGenerator::flakeIdGeneratorXmlGenerator,
                ConfigYamlGenerator::flakeIdGeneratorYamlGenerator
        );
    }

    public static String pnCounterConfigGenerator(PNCounterConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                PN_COUNTER.getName(),
                Config::addPNCounterConfig,
                ConfigXmlGenerator::pnCounterXmlGenerator,
                ConfigYamlGenerator::pnCounterYamlGenerator
        );
    }

    private static <T> String configGenerator(
            T subConfig,
            boolean configIsXml,
            int indent,
            String subConfigTypeName,
            BiConsumer<Config, T> subConfigAdder,
            BiConsumer<ConfigXmlGenerator.XmlGenerator, Config> xmlConfigGenerator,
            BiConsumer<Map<String, Object>, Config> yamlConfigGenerator
    ) {

        String subConfigAsString;

        Config config = new Config();
        subConfigAdder.accept(config, subConfig);

        if (configIsXml) {
            StringBuilder xml = new StringBuilder();
            ConfigXmlGenerator.XmlGenerator gen = new ConfigXmlGenerator.XmlGenerator(xml);

            xmlConfigGenerator.accept(gen, config);

            subConfigAsString = xml.toString();
            subConfigAsString = XmlUtil.format(subConfigAsString, indent);
        } else {
            Map<String, Object> yaml = new LinkedHashMap<>();

            yamlConfigGenerator.accept(yaml, config);

            DumpSettings dumpSettings = DumpSettings.builder()
                    .setDefaultFlowStyle(FlowStyle.BLOCK)
                    .setIndicatorIndent(indent - 2)
                    .setIndent(indent)
                    .build();
            Dump dump = new Dump(dumpSettings);
            subConfigAsString = dump.dumpToString(yaml.get(subConfigTypeName));
        }

        return subConfigAsString;
    }
}
