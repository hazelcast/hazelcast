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

package com.hazelcast.jet.config;

import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class JetConfigXmlGeneratorTest {

    private static final String TO_ESCAPE = "<>&\"'";
    private static final Random RANDOM = new Random();

    @Test
    public void when_defaultJetConfig_thenGeneratesDefaultXml() {
        // When
        JetConfig jetConfig = new JetConfig();
        String xml = generate(jetConfig);

        // Then
        JetConfig generatedConfig = jetConfig(xml);
        assertInstanceConfig(jetConfig.getInstanceConfig(), generatedConfig.getInstanceConfig());
        assertEdgeConfig(jetConfig.getDefaultEdgeConfig(), generatedConfig.getDefaultEdgeConfig());
        assertMetricsConfig(jetConfig.getMetricsConfig(), generatedConfig.getMetricsConfig());
        assertProperties(jetConfig.getProperties(), generatedConfig.getProperties());
    }

    @Test
    public void properties() {
        // When
        JetConfig jetConfig = new JetConfig();
        jetConfig.setProperty("key", "value");
        jetConfig.setProperty("toEscape", TO_ESCAPE);
        String xml = generate(jetConfig, 5);

        // Then
        JetConfig generatedConfig = jetConfig(xml);
        assertProperties(jetConfig.getProperties(), generatedConfig.getProperties());
    }

    @Test
    public void instanceConfig() {
        // When
        JetConfig jetConfig = new JetConfig();
        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        instanceConfig.setCooperativeThreadCount(randomInt())
                      .setBackupCount(randomInt(6))
                      .setScaleUpDelayMillis(randomInt())
                      .setFlowControlPeriodMs(randomInt())
                      .setLosslessRestartEnabled(true);
        String xml = generate(jetConfig);

        // Then
        JetConfig generatedConfig = jetConfig(xml);
        assertInstanceConfig(instanceConfig, generatedConfig.getInstanceConfig());
    }

    @Test
    public void edgeConfig() {
        // When
        JetConfig jetConfig = new JetConfig();
        EdgeConfig defaultEdgeConfig = jetConfig.getDefaultEdgeConfig();
        defaultEdgeConfig.setQueueSize(randomInt())
                         .setPacketSizeLimit(randomInt())
                         .setReceiveWindowMultiplier(randomInt());
        String xml = generate(jetConfig);

        // Then
        JetConfig generatedConfig = jetConfig(xml);
        assertEdgeConfig(defaultEdgeConfig, generatedConfig.getDefaultEdgeConfig());
    }

    @Test
    public void metricsConfig() {
        // When
        JetConfig jetConfig = new JetConfig();
        MetricsConfig metricsConfig = jetConfig.getMetricsConfig();
        metricsConfig.setCollectionIntervalSeconds(randomInt())
                     .setRetentionSeconds(randomInt())
                     .setEnabled(false)
                     .setMetricsForDataStructuresEnabled(true)
                     .setJmxEnabled(false);
        String xml = generate(jetConfig);

        // Then
        JetConfig generatedConfig = jetConfig(xml);
        assertMetricsConfig(metricsConfig, generatedConfig.getMetricsConfig());
    }

    private static void assertProperties(Properties expected, Properties actual) {
        assertEquals(expected.size(), actual.size());
        expected.forEach((key, value) -> assertEquals(value, actual.get(key)));
    }

    private static void assertMetricsConfig(MetricsConfig expected, MetricsConfig actual) {
        assertEquals(expected.getCollectionIntervalSeconds(), actual.getCollectionIntervalSeconds());
        assertEquals(expected.getRetentionSeconds(), actual.getRetentionSeconds());
        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.isMetricsForDataStructuresEnabled(), actual.isMetricsForDataStructuresEnabled());
        assertEquals(expected.isJmxEnabled(), actual.isJmxEnabled());
    }

    private static void assertEdgeConfig(EdgeConfig expected, EdgeConfig actual) {
        assertEquals(expected.getQueueSize(), actual.getQueueSize());
        assertEquals(expected.getPacketSizeLimit(), actual.getPacketSizeLimit());
        assertEquals(expected.getReceiveWindowMultiplier(), actual.getReceiveWindowMultiplier());
    }

    private static void assertInstanceConfig(InstanceConfig expected, InstanceConfig actual) {
        assertEquals(expected.getCooperativeThreadCount(), actual.getCooperativeThreadCount());
        assertEquals(expected.getFlowControlPeriodMs(), actual.getFlowControlPeriodMs());
        assertEquals(expected.getBackupCount(), actual.getBackupCount());
        assertEquals(expected.getScaleUpDelayMillis(), actual.getScaleUpDelayMillis());
        assertEquals(expected.isLosslessRestartEnabled(), actual.isLosslessRestartEnabled());
    }

    private static JetConfig jetConfig(String xml) {
        try {
            File tempFile = File.createTempFile("jet", ".xml");
            try (FileOutputStream os = new FileOutputStream(tempFile)) {
                os.write(Util.readFully(new ByteArrayInputStream(xml.getBytes())));
            }
            return JetConfig.loadFromFile(tempFile);
        } catch (IOException e) {
            ExceptionUtil.sneakyThrow(e);
        }
        return null;
    }

    private static String generate(JetConfig jetConfig) {
        return JetConfigXmlGenerator.generate(jetConfig);
    }

    private static String generate(JetConfig jetConfig, int indent) {
        return JetConfigXmlGenerator.generate(jetConfig, indent);
    }

    private static int randomInt() {
        return randomInt(100) + 1;
    }

    private static int randomInt(int bound) {
        return RANDOM.nextInt(bound) + 1;
    }
}
