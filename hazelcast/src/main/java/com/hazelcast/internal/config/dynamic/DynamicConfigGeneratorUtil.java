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

package com.hazelcast.internal.config.dynamic;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.util.XmlUtil;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public final class DynamicConfigGeneratorUtil {

    private DynamicConfigGeneratorUtil() {
        //not called
    }

    static String wanReplicationConfigGenerator(WanReplicationConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "wan-replication",
                Config::addWanReplicationConfig,
                DynamicConfigXmlGenerator::wanReplicationXmlGenerator,
                DynamicConfigYamlGenerator::wanReplicationYamlGenerator
        );
    }

    static String mapConfigGenerator(MapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "map",
                Config::addMapConfig,
                DynamicConfigXmlGenerator::mapXmlGenerator,
                DynamicConfigYamlGenerator::mapYamlGenerator
        );
    }

    static String cacheConfigGenerator(CacheSimpleConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "cache",
                Config::addCacheConfig,
                DynamicConfigXmlGenerator::cacheXmlGenerator,
                DynamicConfigYamlGenerator::cacheYamlGenerator
        );
    }

    static String queueConfigGenerator(QueueConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "queue",
                Config::addQueueConfig,
                DynamicConfigXmlGenerator::queueXmlGenerator,
                DynamicConfigYamlGenerator::queueYamlGenerator
        );
    }

    static String listConfigGenerator(ListConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "list",
                Config::addListConfig,
                DynamicConfigXmlGenerator::listXmlGenerator,
                DynamicConfigYamlGenerator::listYamlGenerator
        );
    }

    static String setConfigGenerator(SetConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "set",
                Config::addSetConfig,
                DynamicConfigXmlGenerator::setXmlGenerator,
                DynamicConfigYamlGenerator::setYamlGenerator
        );
    }

    static String multiMapConfigGenerator(MultiMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "multimap",
                Config::addMultiMapConfig,
                DynamicConfigXmlGenerator::multiMapXmlGenerator,
                DynamicConfigYamlGenerator::multiMapYamlGenerator
        );
    }

    static String replicatedMapConfigGenerator(ReplicatedMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "replicatedmap",
                Config::addReplicatedMapConfig,
                DynamicConfigXmlGenerator::replicatedMapXmlGenerator,
                DynamicConfigYamlGenerator::replicatedMapYamlGenerator
        );
    }

    static String ringbufferConfigGenerator(RingbufferConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "ringbuffer",
                Config::addRingBufferConfig,
                DynamicConfigXmlGenerator::ringbufferXmlGenerator,
                DynamicConfigYamlGenerator::ringbufferYamlGenerator
        );
    }

    static String topicConfigGenerator(TopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "topic",
                Config::addTopicConfig,
                DynamicConfigXmlGenerator::topicXmlGenerator,
                DynamicConfigYamlGenerator::topicYamlGenerator
        );
    }

    static String reliableTopicConfigGenerator(ReliableTopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "reliable-topic",
                Config::addReliableTopicConfig,
                DynamicConfigXmlGenerator::reliableTopicXmlGenerator,
                DynamicConfigYamlGenerator::reliableTopicYamlGenerator
        );
    }

    static String executorConfigGenerator(ExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "executor-service",
                Config::addExecutorConfig,
                DynamicConfigXmlGenerator::executorXmlGenerator,
                DynamicConfigYamlGenerator::executorYamlGenerator
        );
    }

    static String durableExecutorConfigGenerator(DurableExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "durable-executor-service",
                Config::addDurableExecutorConfig,
                DynamicConfigXmlGenerator::durableExecutorXmlGenerator,
                DynamicConfigYamlGenerator::durableExecutorYamlGenerator
        );
    }

    static String scheduledExecutorConfigGenerator(ScheduledExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "scheduled-executor-service",
                Config::addScheduledExecutorConfig,
                DynamicConfigXmlGenerator::scheduledExecutorXmlGenerator,
                DynamicConfigYamlGenerator::scheduledExecutorYamlGenerator
        );
    }

    static String cardinalityEstimatorConfigGenerator(CardinalityEstimatorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "cardinality-estimator",
                Config::addCardinalityEstimatorConfig,
                DynamicConfigXmlGenerator::cardinalityEstimatorXmlGenerator,
                DynamicConfigYamlGenerator::cardinalityEstimatorYamlGenerator
        );
    }

    static String flakeIdGeneratorConfigGenerator(FlakeIdGeneratorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "flake-id-generator",
                Config::addFlakeIdGeneratorConfig,
                DynamicConfigXmlGenerator::flakeIdGeneratorXmlGenerator,
                DynamicConfigYamlGenerator::flakeIdGeneratorYamlGenerator
        );
    }

    static String pnCounterConfigGenerator(PNCounterConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "pn-counter",
                Config::addPNCounterConfig,
                DynamicConfigXmlGenerator::pnCounterXmlGenerator,
                DynamicConfigYamlGenerator::pnCounterYamlGenerator
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
