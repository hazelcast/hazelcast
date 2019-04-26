/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.AbstractDomConfigProcessor;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import org.w3c.dom.Node;

import java.util.Optional;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.config.DomConfigHelper.getLongValue;
import static com.hazelcast.jet.impl.config.JetConfigSections.EDGE_DEFAULTS;
import static com.hazelcast.jet.impl.config.JetConfigSections.IMPORT;
import static com.hazelcast.jet.impl.config.JetConfigSections.INSTANCE;
import static com.hazelcast.jet.impl.config.JetConfigSections.METRICS;
import static com.hazelcast.jet.impl.config.JetConfigSections.PROPERTIES;
import static com.hazelcast.jet.impl.config.JetConfigSections.canOccurMultipleTimes;

public class JetDomConfigProcessor extends AbstractDomConfigProcessor {

    protected final JetConfig config;

    JetDomConfigProcessor(boolean domLevel3, JetConfig config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException(
                        "Duplicate '" + nodeName + "' definition found in the configuration.");
            }
            if (handleNode(node, nodeName)) {
                continue;
            }
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }

    }

    private boolean handleNode(Node node, String name) {
        if (INSTANCE.isEqual(name)) {
            parseInstanceConfig(node, config);
        } else if (IMPORT.isEqual(name)) {
            throw new HazelcastException("Non-expanded <import> element found");
        } else if (PROPERTIES.isEqual(name)) {
            fillProperties(node, config.getProperties());
        } else if (EDGE_DEFAULTS.isEqual(name)) {
            parseEdgeDefaults(node, config);
        } else if (METRICS.isEqual(name)) {
            parseMetrics(node, config);
        } else {
            return true;
        }
        return false;
    }

    protected void parseInstanceConfig(Node instanceNode, JetConfig config) {
        final InstanceConfig instanceConfig = config.getInstanceConfig();
        for (Node node : childElements(instanceNode)) {
            String name = cleanNodeName(node);
            switch (name) {
                case "cooperative-thread-count":
                    instanceConfig.setCooperativeThreadCount(
                            getIntegerValue("cooperative-thread-count", getTextContent(node))
                    );
                    break;
                case "flow-control-period":
                    instanceConfig.setFlowControlPeriodMs(
                            getIntegerValue("flow-control-period", getTextContent(node))
                    );
                    break;
                case "backup-count":
                    instanceConfig.setBackupCount(
                            getIntegerValue("backup-count", getTextContent(node))
                    );
                    break;
                case "scale-up-delay-millis":
                    instanceConfig.setScaleUpDelayMillis(
                            getLongValue("scale-up-delay-millis", getTextContent(node))
                    );
                    break;
                case "lossless-restart-enabled":
                    instanceConfig.setLosslessRestartEnabled(getBooleanValue(getTextContent(node)));
                    break;
                default:
                    throw new AssertionError("Unrecognized element: " + name);
            }
        }
    }

    protected void parseEdgeDefaults(Node edgeNode, JetConfig config) {
        EdgeConfig edgeConfig = config.getDefaultEdgeConfig();
        for (Node child : childElements(edgeNode)) {
            String name = cleanNodeName(child);
            switch (name) {
                case "queue-size":
                    edgeConfig.setQueueSize(
                            getIntegerValue("queue-size", getTextContent(child))
                    );
                    break;
                case "packet-size-limit":
                    edgeConfig.setPacketSizeLimit(
                            getIntegerValue("packet-size-limit", getTextContent(child))
                    );
                    break;
                case "receive-window-multiplier":
                    edgeConfig.setReceiveWindowMultiplier(
                            getIntegerValue("receive-window-multiplier", getTextContent(child))
                    );
                    break;
                default:
                    throw new AssertionError("Unrecognized element: " + name);
            }
        }
    }

    protected void parseMetrics(Node metricsNode, JetConfig config) {
        MetricsConfig metricsConfig = config.getMetricsConfig();
        getBooleanAttribute(metricsNode, "enabled").ifPresent(metricsConfig::setEnabled);
        getBooleanAttribute(metricsNode, "jmxEnabled").ifPresent(metricsConfig::setJmxEnabled);
        handleMetricsNode(metricsNode, metricsConfig);
    }

    protected void handleMetricsNode(Node metricsNode, MetricsConfig metricsConfig) {
        for (Node child : childElements(metricsNode)) {
            String name = cleanNodeName(child);
            switch (name) {
                case "retention-seconds":
                    metricsConfig.setRetentionSeconds(
                            getIntegerValue("retention-seconds", getTextContent(child))
                    );
                    break;
                case "collection-interval-seconds":
                    metricsConfig.setCollectionIntervalSeconds(
                            getIntegerValue("collection-interval-seconds", getTextContent(child))
                    );
                    break;
                case "metrics-for-data-structures":
                    metricsConfig.setMetricsForDataStructuresEnabled(getBooleanValue(getTextContent(child)));
                    break;
                case "enabled":
                case "jmx-enabled":
                    break;
                default:
                    throw new AssertionError("Unrecognized element: " + name);

            }
        }
    }

    private Optional<Boolean> getBooleanAttribute(Node node, String name) {
        return Optional.ofNullable(node.getAttributes().getNamedItem(name))
                       .map(n -> getBooleanValue(getTextContent(n)));
    }

}
