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
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public final class DynamicConfigGenerator {

    private DynamicConfigGenerator() {
        //not called
    }

    static String queueConfigGenerator(QueueConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "queue",
                Config::addQueueConfig,
                ConfigXmlGenerator::queueXmlGenerator,
                ConfigYamlGenerator::queueYamlGenerator
        );
    }

    static String listConfigGenerator(ListConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "list",
                Config::addListConfig,
                ConfigXmlGenerator::listConfigXmlGenerator,
                ConfigYamlGenerator::listConfigYamlGenerator
        );
    }

    static String setConfigGenerator(SetConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "set",
                Config::addSetConfig,
                ConfigXmlGenerator::setConfigXmlGenerator,
                ConfigYamlGenerator::setConfigYamlGenerator
        );
    }

    static String multiMapConfigGenerator(MultiMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "multimap",
                Config::addMultiMapConfig,
                ConfigXmlGenerator::multiMapXmlGenerator,
                ConfigYamlGenerator::multiMapYamlGenerator
        );
    }

    static String replicatedMapConfigGenerator(ReplicatedMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "replicatedmap",
                Config::addReplicatedMapConfig,
                ConfigXmlGenerator::replicatedMapConfigXmlGenerator,
                ConfigYamlGenerator::replicatedMapConfigYamlGenerator
        );
    }

    static String ringbufferConfigGenerator(RingbufferConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "ringbuffer",
                Config::addRingBufferConfig,
                ConfigXmlGenerator::ringbufferXmlGenerator,
                ConfigYamlGenerator::ringbufferYamlGenerator
        );
    }

    static String topicConfigGenerator(TopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "topic",
                Config::addTopicConfig,
                ConfigXmlGenerator::topicXmlGenerator,
                ConfigYamlGenerator::topicYamlGenerator
        );
    }

    static String reliableTopicConfigGenerator(ReliableTopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "reliable-topic",
                Config::addReliableTopicConfig,
                ConfigXmlGenerator::reliableTopicXmlGenerator,
                ConfigYamlGenerator::reliableTopicYamlGenerator
        );
    }

    static String executorConfigGenerator(ExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "executor-service",
                Config::addExecutorConfig,
                ConfigXmlGenerator::executorXmlGenerator,
                ConfigYamlGenerator::executorYamlGenerator
        );
    }

    static String durableExecutorConfigGenerator(DurableExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "durable-executor-service",
                Config::addDurableExecutorConfig,
                ConfigXmlGenerator::durableExecutorXmlGenerator,
                ConfigYamlGenerator::durableExecutorYamlGenerator
        );
    }

    static String scheduledExecutorConfigGenerator(ScheduledExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "scheduled-executor-service",
                Config::addScheduledExecutorConfig,
                ConfigXmlGenerator::scheduledExecutorXmlGenerator,
                ConfigYamlGenerator::scheduledExecutorYamlGenerator
        );
    }

    static String cardinalityEstimatorConfigGenerator(CardinalityEstimatorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "cardinality-estimator",
                Config::addCardinalityEstimatorConfig,
                ConfigXmlGenerator::cardinalityEstimatorXmlGenerator,
                ConfigYamlGenerator::cardinalityEstimatorYamlGenerator
        );
    }

    static String flakeIdGeneratorConfigGenerator(FlakeIdGeneratorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "flake-id-generator",
                Config::addFlakeIdGeneratorConfig,
                ConfigXmlGenerator::flakeIdGeneratorXmlGenerator,
                ConfigYamlGenerator::flakeIdGeneratorYamlGenerator
        );
    }

    static String pnCounterConfigGenerator(PNCounterConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                "pn-counter",
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
