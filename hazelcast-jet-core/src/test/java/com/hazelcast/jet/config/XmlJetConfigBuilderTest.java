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
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class XmlJetConfigBuilderTest extends AbstractJetConfigBuilderTest {

    public static final String JET_START_TAG = "<hazelcast-jet xmlns=\"http://www.hazelcast.com/schema/jet-config\">\n";
    public static final String JET_END_TAG = "</hazelcast-jet>\n";

    @Test(expected = IllegalArgumentException.class)
    public void testConfiguration_withNullInputStream() {
        new XmlJetConfigBuilder((InputStream) null);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast-jets>"
                + "</hazelcast-jets>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastJetTagAppearsTwice() {
        String xml = JET_START_TAG
                + "    <hazelcast-jet>\n"
                + "    </hazelcast-jet>\n"
                + JET_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void readMetricsConfig() {
        //Given
        String xml = JET_START_TAG +
                "   <metrics enabled=\"false\" jmxEnabled=\"false\">\n" +
                "        <collection-interval-seconds>123</collection-interval-seconds>\n" +
                "        <retention-seconds>124</retention-seconds>\n" +
                "        <metrics-for-data-structures>true</metrics-for-data-structures>\n" +
                "    </metrics>\n\n" +
                JET_END_TAG;

        //When
        JetConfig jetConfig = buildConfig(xml);

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsRetentionSeconds", 124, metricsCfg.getRetentionSeconds());
        assertEquals("metricsCollectionInterval", 123, metricsCfg.getCollectionIntervalSeconds());
        assertTrue("metricsForDataStructures", metricsCfg.isMetricsForDataStructuresEnabled());
    }

    @Test
    public void readInstanceConfig() {
        //Given
        String xml = JET_START_TAG +
                "    <instance>\n" +
                "        <cooperative-thread-count>66</cooperative-thread-count>\n" +
                "        <flow-control-period>51</flow-control-period>\n" +
                "        <backup-count>4</backup-count>\n" +
                "        <scale-up-delay-millis>1234</scale-up-delay-millis>\n" +
                "        <lossless-restart-enabled>true</lossless-restart-enabled>\n" +
                "    </instance>\n" +
                JET_END_TAG;

        //When
        JetConfig jetConfig = buildConfig(xml);

        //Then
        assertEquals("cooperativeThreadCount", 66, jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals("backupCount", 4, jetConfig.getInstanceConfig().getBackupCount());
        assertEquals("flowControlMs", 51, jetConfig.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, jetConfig.getInstanceConfig().getScaleUpDelayMillis());
        assertTrue("losslessRestartEnabled", jetConfig.getInstanceConfig().isLosslessRestartEnabled());
    }

    @Test
    public void readEdgeDefaults() {
        //Given
        String xml = JET_START_TAG +
                "    <edge-defaults>\n" +
                "       <queue-size>111</queue-size>\n" +
                "       <packet-size-limit>222</packet-size-limit>\n" +
                "       <receive-window-multiplier>333</receive-window-multiplier>\n" +
                "    </edge-defaults>\n" +
                JET_END_TAG;

        //When
        JetConfig jetConfig = buildConfig(xml);

        //Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 111, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 222, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 333, edgeConfig.getReceiveWindowMultiplier());
    }

    @Test
    public void readProperties() {
        //Given
        String xml = JET_START_TAG +
                "    <properties>\n" +
                "       <property name=\"property1\">value1</property>\n" +
                "       <property name=\"property2\">value2</property>\n" +
                "    </properties>\n" +
                JET_END_TAG;

        //When
        JetConfig jetConfig = buildConfig(xml);

        //Then
        assertEquals("value1", jetConfig.getProperties().getProperty("property1"));
        assertEquals("value2", jetConfig.getProperties().getProperty("property2"));
    }

    @Override
    protected JetConfig buildConfig(String configuration) {
        ByteArrayInputStream bis = new ByteArrayInputStream(configuration.getBytes());
        XmlJetConfigBuilder configBuilder = new XmlJetConfigBuilder(bis);
        return configBuilder.build();
    }

}
