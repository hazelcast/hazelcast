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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class YamlJetConfigBuilderTest extends AbstractJetConfigBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConfiguration_withNullInputStream() {
        new YamlJetConfigBuilder((InputStream) null);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        //Given
        String yaml = ""
                + "hazelcast-jet1:\n"
                + "  test:\n"
                + "    name: dev\n";

        //When
        buildConfig(yaml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastJetTagAppearsTwice() {
        //Given
        String yaml = ""
                + "hazelcast-jet: {}\n"
                + "hazelcast-jet: {}";

        //When
        buildConfig(yaml);
    }

    @Test
    public void readMetricsConfig() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "    enabled: false\n"
                + "    jmx-enabled: true\n"
                + "    retention-seconds: 124\n"
                + "    metrics-for-data-structures: true\n"
                + "    collection-interval-seconds: 123\n";

        //When
        JetConfig jetConfig = buildConfig(yaml);

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertTrue("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsRetentionSeconds", 124, metricsCfg.getRetentionSeconds());
        assertEquals("metricsCollectionInterval", 123, metricsCfg.getCollectionIntervalSeconds());
        assertTrue("metricsForDataStructures", metricsCfg.isMetricsForDataStructuresEnabled());
    }

    @Test
    public void readInstanceConfig() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  instance:\n"
                + "    cooperative-thread-count: 55\n"
                + "    flow-control-period: 50\n"
                + "    backup-count: 2\n"
                + "    scale-up-delay-millis: 1234\n"
                + "    lossless-restart-enabled: true\n";

        //When
        JetConfig jetConfig = buildConfig(yaml);

        //Then
        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        assertEquals("cooperativeThreadCount", 55, instanceConfig.getCooperativeThreadCount());
        assertEquals("backupCount", 2, instanceConfig.getBackupCount());
        assertEquals("flowControlMs", 50, instanceConfig.getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, instanceConfig.getScaleUpDelayMillis());
        assertTrue("losslessRestartEnabled", instanceConfig.isLosslessRestartEnabled());

    }

    @Test
    public void readEdgeDefaults() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  edge-defaults:\n"
                + "    queue-size: 999\n"
                + "    packet-size-limit: 997\n"
                + "    receive-window-multiplier: 996\n";

        //When
        JetConfig jetConfig = buildConfig(yaml);

        //Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 999, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 997, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 996, edgeConfig.getReceiveWindowMultiplier());
    }


    @Test
    public void readProperties() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    property1: value1\n"
                + "    property2: value2\n";

        //When
        JetConfig jetConfig = buildConfig(yaml);

        //Then
        Properties properties = jetConfig.getProperties();
        assertEquals("value1", properties.getProperty("property1"));
        assertEquals("value2", properties.getProperty("property2"));
    }

    @Test
    public void testNullInMapThrows() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    test:\n"
                + "    test1: test2\n";
        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-jet/properties/test"));

        //When
        buildConfig(yaml);
    }

    @Test
    public void testExplicitNullScalarThrows() {
        //Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "     retention-seconds: !!null";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class,
                "hazelcast-jet/metrics/retention-seconds"));

        //When
        buildConfig(yaml);
    }

    @Override
    protected JetConfig buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlJetConfigBuilder configBuilder = new YamlJetConfigBuilder(bis);
        return configBuilder.build();
    }
}
