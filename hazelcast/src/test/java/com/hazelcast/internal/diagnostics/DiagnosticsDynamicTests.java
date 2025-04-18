/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiagnosticsConfig;
import com.hazelcast.config.DiagnosticsOutputType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsDynamicTests extends AbstractDiagnosticsPluginTest {
    private HazelcastInstance hz;
    Map<String, String> properties = new ConcurrentHashMap<>();
    Diagnostics diagnostics;
    // source of truth for expected configs
    HazelcastProperties expectedHzProperties;

    public enum ConfigSources {
        Props,
        Config,
        Both
    }

    @Parameterized.Parameter
    public ConfigSources configSource;

    @Parameterized.Parameters(name = "configSource:{0}")
    public static Iterable<ConfigSources> parameters() {
        return new ArrayList<>(Arrays.asList(ConfigSources.values()));
    }

    @Before
    public void setup() {
        Config config = new Config();
        config.getDiagnosticsConfig().setEnabled(false);
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED.getName(), "true");

        properties.put(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "1");
        properties.put(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName(), "2");
        properties.put(InvocationSamplePlugin.SLOW_MAX_COUNT.getName(), "100");

        properties.put(EventQueuePlugin.PERIOD_SECONDS.getName(), "1");
        properties.put(EventQueuePlugin.THRESHOLD.getName(), "1000");
        properties.put(EventQueuePlugin.SAMPLES.getName(), "100");

        properties.put(InvocationProfilerPlugin.PERIOD_SECONDS.getName(), "1");

        properties.put(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName(), "1");

        properties.put(MemberHeartbeatPlugin.PERIOD_SECONDS.getName(), "1");
        properties.put(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(), "42");

        properties.put(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        properties.put(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "1");

        properties.put(OperationHeartbeatPlugin.PERIOD_SECONDS.getName(), "1");
        properties.put(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(), "42");

        properties.put(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "1");

        properties.put(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName(), "1");
        properties.put(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName(), "100");
        properties.put(OperationThreadSamplerPlugin.INCLUDE_NAME.getName(), "false");

        properties.put(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(), "5");
        properties.put(OverloadedConnectionsPlugin.THRESHOLD.getName(), "10000");
        properties.put(OverloadedConnectionsPlugin.SAMPLES.getName(), "1000");

        properties.put(PendingInvocationsPlugin.PERIOD_SECONDS.getName(), "10");
        properties.put(PendingInvocationsPlugin.THRESHOLD.getName(), "10");

        properties.put(SlowOperationPlugin.PERIOD_SECONDS.getName(), "10");

        properties.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "10");
        properties.put(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(), "15");

        properties.put(SystemLogPlugin.ENABLED.getName(), "true");
        properties.put(SystemLogPlugin.LOG_PARTITIONS.getName(), "true");

        if (configSource == ConfigSources.Config) {
            config.getDiagnosticsConfig().getPluginProperties().putAll(properties);
        } else if (configSource == ConfigSources.Props) {
            setAsProperty(config);
        } else if (configSource == ConfigSources.Both) {
            // system properties take precedence
            setAsProperty(config);

            Map<String, String> corruptedProperties = new HashMap<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                corruptedProperties.put(entry.getKey(),
                        (entry.getValue().equals("true") || entry.getValue().equals("false")) ? "false" : "999");
            }
            // this should be ignored since system properties take precedence
            config.getDiagnosticsConfig().getPluginProperties().putAll(corruptedProperties);
        }

        hz = createHazelcastInstance(config);
        diagnostics = TestUtil.getNode(hz).getNodeEngine().getDiagnostics();

        // source of truth for expected configs
        Properties sourceProps = new Properties();
        sourceProps.putAll(properties);
        expectedHzProperties = new HazelcastProperties(sourceProps);
    }

    private void setAsProperty(Config config) {
        Properties systemProps = new Properties();
        systemProps.putAll(properties);
        config.setProperties(systemProps);
    }

    @Test
    public void testDiagnosticsDynamicallyEnabled() {
        assertInvocationSamplePlugin(DiagnosticsPlugin.DISABLED);
        assertBuildInfoPlugin(DiagnosticsPlugin.DISABLED);
        assertConfigPropertiesPlugin(DiagnosticsPlugin.DISABLED);
        assertEventQueuePlugin(DiagnosticsPlugin.DISABLED);
        assertInvocationProfilerPlugin(DiagnosticsPlugin.DISABLED);
        assertMemberHazelcastInstanceInfoPlugin(DiagnosticsPlugin.DISABLED);
        assertMemberHeartbeatPlugin(DiagnosticsPlugin.DISABLED);
        assertMetricsPlugin(DiagnosticsPlugin.DISABLED);
        assertNetworkingImbalancePlugin(DiagnosticsPlugin.DISABLED);
        assertOperationHeartbeatPlugin(DiagnosticsPlugin.DISABLED);
        assertOperationProfilerPlugin(DiagnosticsPlugin.DISABLED);
        assertOperationProfilerPlugin(DiagnosticsPlugin.DISABLED);
        assertOverloadedConnectionsPlugin(DiagnosticsPlugin.DISABLED);
        assertPendingInvocationsPlugin(DiagnosticsPlugin.DISABLED);
        assertSlowOperationPlugin(DiagnosticsPlugin.DISABLED);
        assertStoreLatencyPlugin(DiagnosticsPlugin.DISABLED);
        assertSystemLogPlugin(DiagnosticsPlugin.DISABLED);
        assertSystemPropertiesPlugin(DiagnosticsPlugin.DISABLED);

        enableDiagnostics();
        assertInvocationSamplePlugin(DiagnosticsPlugin.RUNNING);
        assertBuildInfoPlugin(DiagnosticsPlugin.RUNNING);
        assertConfigPropertiesPlugin(DiagnosticsPlugin.RUNNING);
        assertEventQueuePlugin(DiagnosticsPlugin.RUNNING);
        assertInvocationProfilerPlugin(DiagnosticsPlugin.RUNNING);
        assertMemberHazelcastInstanceInfoPlugin(DiagnosticsPlugin.RUNNING);
        assertMemberHeartbeatPlugin(DiagnosticsPlugin.RUNNING);
        assertMetricsPlugin(DiagnosticsPlugin.RUNNING);
        assertNetworkingImbalancePlugin(DiagnosticsPlugin.RUNNING);
        assertOperationHeartbeatPlugin(DiagnosticsPlugin.RUNNING);
        assertOperationProfilerPlugin(DiagnosticsPlugin.RUNNING);
        assertOperationThreadSamplerPlugin(DiagnosticsPlugin.RUNNING);
        assertOverloadedConnectionsPlugin(DiagnosticsPlugin.RUNNING);
        assertPendingInvocationsPlugin(DiagnosticsPlugin.RUNNING);
        assertSlowOperationPlugin(DiagnosticsPlugin.RUNNING);
        assertStoreLatencyPlugin(DiagnosticsPlugin.RUNNING);
        assertSystemLogPlugin(DiagnosticsPlugin.RUNNING);
        assertSystemPropertiesPlugin(DiagnosticsPlugin.RUNNING);

        disableDiagnostics();
        assertInvocationSamplePlugin(DiagnosticsPlugin.DISABLED);
        assertBuildInfoPlugin(DiagnosticsPlugin.DISABLED);
        assertConfigPropertiesPlugin(DiagnosticsPlugin.DISABLED);
        assertEventQueuePlugin(DiagnosticsPlugin.DISABLED);
        assertInvocationProfilerPlugin(DiagnosticsPlugin.DISABLED);
        assertMemberHazelcastInstanceInfoPlugin(DiagnosticsPlugin.DISABLED);
        assertMemberHeartbeatPlugin(DiagnosticsPlugin.DISABLED);
        assertMetricsPlugin(DiagnosticsPlugin.DISABLED);
        assertNetworkingImbalancePlugin(DiagnosticsPlugin.DISABLED);
        assertOperationHeartbeatPlugin(DiagnosticsPlugin.DISABLED);
        assertOperationProfilerPlugin(DiagnosticsPlugin.DISABLED);
        assertOperationThreadSamplerPlugin(DiagnosticsPlugin.DISABLED);
        assertOverloadedConnectionsPlugin(DiagnosticsPlugin.DISABLED);
        assertPendingInvocationsPlugin(DiagnosticsPlugin.DISABLED);
        assertSlowOperationPlugin(DiagnosticsPlugin.DISABLED);
        assertStoreLatencyPlugin(DiagnosticsPlugin.DISABLED);
        assertSystemLogPlugin(DiagnosticsPlugin.DISABLED);
        assertSystemPropertiesPlugin(DiagnosticsPlugin.DISABLED);

    }

    @Test
    public void testOutputChanges() throws IOException {
        DiagnosticsConfig dConfig = new DiagnosticsConfig();
        dConfig.setOutputType(DiagnosticsOutputType.STDOUT);
        dConfig.setEnabled(true);

        setDiagnosticsConfig(dConfig);

        assertTrue(diagnostics.diagnosticsLog instanceof DiagnosticsStdout);

        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        dConfig.setOutputType(DiagnosticsOutputType.FILE);
        dConfig.setLogDirectory(tmpFolder.getRoot().getAbsolutePath());

        setDiagnosticsConfig(dConfig);

        assertTrueEventually(() -> {
            assertEquals(tmpFolder.getRoot().getAbsolutePath(), diagnostics.getLoggingDirectory().getAbsolutePath());
            assertTrue(tmpFolder.getRoot().listFiles().length > 0);
            assertStartsWith(diagnostics.getBaseFileNameWithTime(), tmpFolder.getRoot().listFiles()[0].getName());
            assertGreaterOrEquals("log file size", tmpFolder.getRoot().listFiles()[0].length(), 1);
        });
    }

    private void enableDiagnostics() {
        DiagnosticsConfig dConfig = new DiagnosticsConfig();
        dConfig.setEnabled(true);
        dConfig.getPluginProperties().putAll(properties);
        setDiagnosticsConfig(dConfig);
    }

    private void setDiagnosticsConfig(DiagnosticsConfig dConfig) {
        diagnostics.setConfig(dConfig);
        assertTrue(diagnostics.isEnabled());
    }

    private void disableDiagnostics() {
        DiagnosticsConfig dConfig = new DiagnosticsConfig();
        dConfig.setEnabled(false);
        dConfig.getPluginProperties().putAll(properties);
        diagnostics.setConfig(dConfig);
        assertFalse(diagnostics.isEnabled());
    }

    void assertInvocationSamplePlugin(long status) {
        InvocationSamplePlugin plugin = getPlugin(InvocationSamplePlugin.class);

        // expected properties
        HazelcastProperty samplePeriod = new HazelcastProperty(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty slowThreshold = new HazelcastProperty(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName())), TimeUnit.SECONDS);

        HazelcastProperty maxCount = new HazelcastProperty(InvocationSamplePlugin.SLOW_MAX_COUNT.getName(),
                properties.get(InvocationSamplePlugin.SLOW_MAX_COUNT.getName()));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod),
                plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(slowThreshold),
                plugin.getThresholdMillis());
        assertEquals(expectedHzProperties.getInteger(maxCount),
                plugin.getMaxCount());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
        }
    }

    void assertEventQueuePlugin(long status) {
        EventQueuePlugin plugin = getPlugin(EventQueuePlugin.class);

        // expected properties
        HazelcastProperty period = new HazelcastProperty(EventQueuePlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(EventQueuePlugin.PERIOD_SECONDS.getName())), TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(EventQueuePlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(EventQueuePlugin.THRESHOLD.getName())));

        HazelcastProperty samples = new HazelcastProperty(EventQueuePlugin.SAMPLES.getName(),
                properties.get(EventQueuePlugin.SAMPLES.getName()));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period),
                plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold),
                plugin.getThreshold());
        assertEquals(expectedHzProperties.getInteger(samples),
                plugin.getSamples());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(EventQueuePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(EventQueuePlugin.class));
        }
    }

    void assertBuildInfoPlugin(long status) {
        BuildInfoPlugin plugin = getPlugin(BuildInfoPlugin.class);
        assertEquals(status, plugin.isRunning.get());
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    void assertConfigPropertiesPlugin(long status) {
        ConfigPropertiesPlugin plugin = (ConfigPropertiesPlugin) getPlugin(ConfigPropertiesPlugin.class);
        assertEquals(status, plugin.isRunning.get());
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    void assertInvocationProfilerPlugin(long status) {
        InvocationProfilerPlugin plugin = (InvocationProfilerPlugin) getPlugin(InvocationProfilerPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(InvocationProfilerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationProfilerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);


        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(InvocationProfilerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(InvocationProfilerPlugin.class));
        }
    }

    void assertMemberHazelcastInstanceInfoPlugin(long status) {
        MemberHazelcastInstanceInfoPlugin plugin = getPlugin(MemberHazelcastInstanceInfoPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(MemberHazelcastInstanceInfoPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MemberHazelcastInstanceInfoPlugin.class));
        }
    }

    void assertMemberHeartbeatPlugin(long status) {
        MemberHeartbeatPlugin plugin = getPlugin(MemberHeartbeatPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MemberHeartbeatPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MemberHeartbeatPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty maxDeviation = new HazelcastProperty(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(),
                Integer.valueOf(properties.get(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(maxDeviation), plugin.getMaxDeviationPercentage());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(MemberHeartbeatPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MemberHeartbeatPlugin.class));
        }
    }

    void assertOperationHeartbeatPlugin(long status) {
        OperationHeartbeatPlugin plugin = getPlugin(OperationHeartbeatPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(OperationHeartbeatPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationHeartbeatPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty maxDeviation = new HazelcastProperty(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(),
                Integer.valueOf(properties.get(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(maxDeviation), plugin.getMaxDeviationPercentage());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(OperationHeartbeatPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationHeartbeatPlugin.class));
        }
    }

    void assertMetricsPlugin(long status) {
        MetricsPlugin plugin = getPlugin(MetricsPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MetricsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MetricsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(MetricsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MetricsPlugin.class));
        }
    }

    void assertNetworkingImbalancePlugin(long status) {
        NetworkingImbalancePlugin plugin = getPlugin(NetworkingImbalancePlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(NetworkingImbalancePlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        Server server = TestUtil.getNode(hz).getServer();

        if ((server instanceof TcpServer tcpServer) && (tcpServer.getNetworking() instanceof NioNetworking)) {
            assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
            assertEquals(status, plugin.isRunning.get());
        } else {
            // networking is null
            assertEquals(0, plugin.getPeriodMillis());
            assertEquals(DiagnosticsPlugin.DISABLED, plugin.isRunning.get());
        }

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(NetworkingImbalancePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(NetworkingImbalancePlugin.class));
        }
    }

    void assertOperationProfilerPlugin(long status) {
        OperationProfilerPlugin plugin = getPlugin(OperationProfilerPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(OperationProfilerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationProfilerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(OperationProfilerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationProfilerPlugin.class));
        }
    }

    void assertOperationThreadSamplerPlugin(long status) {
        OperationThreadSamplerPlugin plugin = getPlugin(OperationThreadSamplerPlugin.class);

        HazelcastProperty period = new HazelcastProperty(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty samplerPeriod = new HazelcastProperty(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName(),
                Integer.valueOf(properties.get(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName())),
                TimeUnit.MILLISECONDS);

        HazelcastProperty includeName = new HazelcastProperty(OperationThreadSamplerPlugin.INCLUDE_NAME.getName(),
                Boolean.valueOf(properties.get(OperationThreadSamplerPlugin.INCLUDE_NAME.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(samplerPeriod), plugin.getSamplerPeriodMillis());
        assertEquals(expectedHzProperties.getBoolean(includeName), plugin.getIncludeName());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(OperationThreadSamplerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationThreadSamplerPlugin.class));
        }
    }

    void assertOverloadedConnectionsPlugin(long status) {
        OverloadedConnectionsPlugin plugin = getPlugin(OverloadedConnectionsPlugin.class);

        HazelcastProperty period = new HazelcastProperty(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(OverloadedConnectionsPlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.THRESHOLD.getName())));

        HazelcastProperty samples = new HazelcastProperty(OverloadedConnectionsPlugin.SAMPLES.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.SAMPLES.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold), plugin.getThreshold());
        assertEquals(expectedHzProperties.getInteger(samples), plugin.getSamples());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(OverloadedConnectionsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OverloadedConnectionsPlugin.class));
        }
    }

    void assertPendingInvocationsPlugin(long status) {
        PendingInvocationsPlugin plugin = getPlugin(PendingInvocationsPlugin.class);

        HazelcastProperty period = new HazelcastProperty(PendingInvocationsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(PendingInvocationsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(PendingInvocationsPlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(PendingInvocationsPlugin.THRESHOLD.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold), plugin.getThreshold());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(PendingInvocationsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(PendingInvocationsPlugin.class));
        }
    }

    void assertSlowOperationPlugin(long status) {
        SlowOperationPlugin plugin = getPlugin(SlowOperationPlugin.class);

        HazelcastProperty period = new HazelcastProperty(SlowOperationPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(SlowOperationPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(SlowOperationPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(SlowOperationPlugin.class));
        }
    }

    void assertStoreLatencyPlugin(long status) {
        StoreLatencyPlugin plugin = getPlugin(StoreLatencyPlugin.class);

        HazelcastProperty period = new HazelcastProperty(StoreLatencyPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(StoreLatencyPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty resetPeriod = new HazelcastProperty(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(resetPeriod), plugin.getResetPeriodMillis());

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(StoreLatencyPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(StoreLatencyPlugin.class));
        }
    }

    void assertSystemLogPlugin(long status) {
        SystemLogPlugin plugin = getPlugin(SystemLogPlugin.class);

        HazelcastProperty enabled = new HazelcastProperty(SystemLogPlugin.ENABLED.getName(),
                Boolean.valueOf(properties.get(SystemLogPlugin.ENABLED.getName())));

        HazelcastProperty logPartitions = new HazelcastProperty(SystemLogPlugin.LOG_PARTITIONS.getName(),
                Boolean.valueOf(properties.get(SystemLogPlugin.LOG_PARTITIONS.getName())));

        assertEquals(status, plugin.isRunning.get());
        assertEquals(expectedHzProperties.getBoolean(logPartitions), plugin.getLogPartitions());
        assertEquals(expectedHzProperties.getBoolean(enabled), plugin.getPeriodMillis() == 1000);

        if (plugin.isRunning()) {
            assertNotNull(diagnostics.getFutureOf(SystemLogPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(SystemLogPlugin.class));
        }
    }

    void assertSystemPropertiesPlugin(long status) {
        SystemPropertiesPlugin plugin = getPlugin(SystemPropertiesPlugin.class);
        assertEquals(status, plugin.isRunning.get());
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    private <P extends DiagnosticsPlugin> P getPlugin(Class<P> clazz) {
        // have to use reflection since the service will return null when it's not enabled.
        Method getPluginInstanceMethod = null;
        try {
            getPluginInstanceMethod = Diagnostics.class.getDeclaredMethod("getPluginInstance", Class.class);
            getPluginInstanceMethod.setAccessible(true);
            return (P) getPluginInstanceMethod.invoke(diagnostics, clazz);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
