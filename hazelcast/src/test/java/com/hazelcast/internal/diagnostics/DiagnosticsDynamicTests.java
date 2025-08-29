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
import org.junit.Assume;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

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
        PROPS,
        NONE,
        DisabledStatically,
        EnabledStatically
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
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED.getName(), "true");

        if (configSource == ConfigSources.DisabledStatically) {
            properties.put(Diagnostics.ENABLED.getName(), "false");
        } else if (configSource == ConfigSources.EnabledStatically) {
            properties.put(Diagnostics.ENABLED.getName(), "true");
        }

        setDefaultProperties();

        setPropertiesByTestCase(config);

        hz = createHazelcastInstance(config);
        diagnostics = TestUtil.getNode(hz).getNodeEngine().getDiagnostics();
    }

    private void setPropertiesByTestCase(Config config) {
        if (configSource == ConfigSources.PROPS || configSource == ConfigSources.DisabledStatically
                || configSource == ConfigSources.EnabledStatically) {
            setAsProperty(config);
        }
    }

    private void setDefaultProperties() {
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

        Properties sourceProps = new Properties();
        sourceProps.putAll(properties);
        expectedHzProperties = new HazelcastProperties(sourceProps);
    }

    private void alterProperties() {
        properties.put(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "2");
        properties.put(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName(), "3");
        properties.put(InvocationSamplePlugin.SLOW_MAX_COUNT.getName(), "101");

        properties.put(EventQueuePlugin.PERIOD_SECONDS.getName(), "2");
        properties.put(EventQueuePlugin.THRESHOLD.getName(), "1001");
        properties.put(EventQueuePlugin.SAMPLES.getName(), "101");

        properties.put(InvocationProfilerPlugin.PERIOD_SECONDS.getName(), "2");

        properties.put(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName(), "2");

        properties.put(MemberHeartbeatPlugin.PERIOD_SECONDS.getName(), "2");
        properties.put(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(), "43");

        properties.put(MetricsPlugin.PERIOD_SECONDS.getName(), "2");

        properties.put(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "2");

        properties.put(OperationHeartbeatPlugin.PERIOD_SECONDS.getName(), "2");
        properties.put(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(), "43");

        properties.put(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "2");

        properties.put(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName(), "2");
        properties.put(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName(), "101");
        properties.put(OperationThreadSamplerPlugin.INCLUDE_NAME.getName(), "true");

        properties.put(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(), "6");
        properties.put(OverloadedConnectionsPlugin.THRESHOLD.getName(), "10001");
        properties.put(OverloadedConnectionsPlugin.SAMPLES.getName(), "1001");

        properties.put(PendingInvocationsPlugin.PERIOD_SECONDS.getName(), "11");
        properties.put(PendingInvocationsPlugin.THRESHOLD.getName(), "11");

        properties.put(SlowOperationPlugin.PERIOD_SECONDS.getName(), "11");

        properties.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "11");
        properties.put(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(), "16");

        properties.put(SystemLogPlugin.ENABLED.getName(), "false");
        properties.put(SystemLogPlugin.LOG_PARTITIONS.getName(), "false");

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
    public void testDiagnosticsDynamicallyEnabled_butPluginsDisabled() {
        // props cannot be overridden by config object at runtime
        Assume.assumeTrue(configSource == ConfigSources.NONE);

        enableDiagnostics();
        assertInvocationSamplePlugin(true);
        assertBuildInfoPlugin(true);
        assertConfigPropertiesPlugin(true);
        assertEventQueuePlugin(true);
        assertInvocationProfilerPlugin(true);
        assertMemberHazelcastInstanceInfoPlugin(true);
        assertMemberHeartbeatPlugin(true);
        assertMetricsPlugin(true);
        assertNetworkingImbalancePlugin(true);
        assertOperationHeartbeatPlugin(true);
        assertOperationProfilerPlugin(true);
        assertOperationThreadSamplerPlugin(true);
        assertOverloadedConnectionsPlugin(true);
        assertPendingInvocationsPlugin(true);
        assertSlowOperationPlugin(true);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(true);
        assertSystemPropertiesPlugin(true);
        assertGreaterOrEquals("Diagnostics dynamically enabled count",
                diagnostics.getMetricCollector().getDynamicallyEnabledCount().get(), 1);

        // The service will be enabled, but the plugins will not
        disablePlugins();
        enableDiagnostics();
        assertTrue(diagnostics.isEnabled());
        assertInvocationSamplePlugin(false);
        // static plugin, just enabled
        assertBuildInfoPlugin(true);
        // static plugin, just enabled
        assertConfigPropertiesPlugin(true);
        assertEventQueuePlugin(false);
        assertInvocationProfilerPlugin(false);
        assertMemberHazelcastInstanceInfoPlugin(false);
        assertMemberHeartbeatPlugin(false);
        assertMetricsPlugin(false);
        assertNetworkingImbalancePlugin(false);
        assertOperationHeartbeatPlugin(false);
        assertOperationProfilerPlugin(false);
        assertOperationProfilerPlugin(false);
        assertOverloadedConnectionsPlugin(false);
        assertPendingInvocationsPlugin(false);
        assertSlowOperationPlugin(false);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(false);
        assertSystemPropertiesPlugin(true);
    }

    @Test
    public void testDiagnosticsDynamicallyEnabled() {
        Assume.assumeTrue(configSource == ConfigSources.PROPS);
        assertInvocationSamplePlugin(false);
        assertBuildInfoPlugin(false);
        assertConfigPropertiesPlugin(false);
        assertEventQueuePlugin(false);
        assertInvocationProfilerPlugin(false);
        assertMemberHazelcastInstanceInfoPlugin(false);
        assertMemberHeartbeatPlugin(false);
        assertMetricsPlugin(false);
        assertNetworkingImbalancePlugin(false);
        assertOperationHeartbeatPlugin(false);
        assertOperationProfilerPlugin(false);
        assertOperationProfilerPlugin(false);
        assertOverloadedConnectionsPlugin(false);
        assertPendingInvocationsPlugin(false);
        assertSlowOperationPlugin(false);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(false);
        assertSystemPropertiesPlugin(false);

        enableDiagnostics();
        assertInvocationSamplePlugin(true);
        assertBuildInfoPlugin(true);
        assertConfigPropertiesPlugin(true);
        assertEventQueuePlugin(true);
        assertInvocationProfilerPlugin(true);
        assertMemberHazelcastInstanceInfoPlugin(true);
        assertMemberHeartbeatPlugin(true);
        assertMetricsPlugin(true);
        assertNetworkingImbalancePlugin(true);
        assertOperationHeartbeatPlugin(true);
        assertOperationProfilerPlugin(true);
        assertOperationThreadSamplerPlugin(true);
        assertOverloadedConnectionsPlugin(true);
        assertPendingInvocationsPlugin(true);
        assertSlowOperationPlugin(true);
        // store latency cannot be enabled dynamically
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(true);
        assertSystemPropertiesPlugin(true);

        disableDiagnostics();
        assertInvocationSamplePlugin(false);
        assertBuildInfoPlugin(false);
        assertConfigPropertiesPlugin(false);
        assertEventQueuePlugin(false);
        assertInvocationProfilerPlugin(false);
        assertMemberHazelcastInstanceInfoPlugin(false);
        assertMemberHeartbeatPlugin(false);
        assertMetricsPlugin(false);
        assertNetworkingImbalancePlugin(false);
        assertOperationHeartbeatPlugin(false);
        assertOperationProfilerPlugin(false);
        assertOperationThreadSamplerPlugin(false);
        assertOverloadedConnectionsPlugin(false);
        assertPendingInvocationsPlugin(false);
        assertSlowOperationPlugin(false);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(false);
        assertSystemPropertiesPlugin(false);

    }

    @Test
    public void testDiagnosticsDynamicallyUpdateProps() {
        // this test is only valid over dynamic updates
        // diagnostics cannot be configured over system properties dynamically
        Assume.assumeTrue(configSource == ConfigSources.NONE);

        enableDiagnostics();
        assertInvocationSamplePlugin(true);
        assertBuildInfoPlugin(true);
        assertConfigPropertiesPlugin(true);
        assertEventQueuePlugin(true);
        assertInvocationProfilerPlugin(true);
        assertMemberHazelcastInstanceInfoPlugin(true);
        assertMemberHeartbeatPlugin(true);
        assertMetricsPlugin(true);
        assertNetworkingImbalancePlugin(true);
        assertOperationHeartbeatPlugin(true);
        assertOperationProfilerPlugin(true);
        assertOperationThreadSamplerPlugin(true);
        assertOverloadedConnectionsPlugin(true);
        assertPendingInvocationsPlugin(true);
        assertSlowOperationPlugin(true);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(true);
        assertSystemPropertiesPlugin(true);

        alterProperties();
        enableDiagnostics();
        assertInvocationSamplePlugin(true);
        assertBuildInfoPlugin(true);
        assertConfigPropertiesPlugin(true);
        assertEventQueuePlugin(true);
        assertInvocationProfilerPlugin(true);
        assertMemberHazelcastInstanceInfoPlugin(true);
        assertMemberHeartbeatPlugin(true);
        assertMetricsPlugin(true);
        assertNetworkingImbalancePlugin(true);
        assertOperationHeartbeatPlugin(true);
        assertOperationProfilerPlugin(true);
        assertOperationThreadSamplerPlugin(true);
        assertOverloadedConnectionsPlugin(true);
        assertPendingInvocationsPlugin(true);
        assertSlowOperationPlugin(true);
        assertStoreLatencyPlugin();
        assertSystemLogPlugin(false);
        assertSystemPropertiesPlugin(true);
    }

    @Test
    public void testOutputChanges() throws IOException {
        Assume.assumeFalse(configSource == ConfigSources.DisabledStatically
                || configSource == ConfigSources.EnabledStatically);

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

    @Test
    public void testOverriddenPropertiesLogged() {
        Assume.assumeFalse(configSource == ConfigSources.DisabledStatically
                || configSource == ConfigSources.EnabledStatically);

        StringBuilder sb = new StringBuilder();
        hz.getLoggingService().addLogListener(Level.INFO,
                message -> sb.append(message.getLogRecord().getMessage()).append(System.lineSeparator()));

        DiagnosticsConfig dConfig = new DiagnosticsConfig();
        properties.put(Diagnostics.FILENAME_PREFIX.getName(), "hz-prefix");
        properties.put(Diagnostics.INCLUDE_EPOCH_TIME.getName(), "true");
        properties.put(Diagnostics.MAX_ROLLED_FILE_COUNT.getName(), "5");
        properties.put(Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName(), "10");
        properties.put(Diagnostics.DIRECTORY.getName(), dConfig.getLogDirectory());
        properties.put(Diagnostics.OUTPUT_TYPE.getName(), "FILE");

        // Prepare and inject the properties
        Properties props = new Properties();
        props.putAll(properties);
        diagnostics.hazelcastProperties = new HazelcastProperties(props);

        dConfig.setEnabled(true);
        dConfig.setMaxRolledFileCount(1);
        dConfig.setMaxRolledFileSizeInMB(1);
        dConfig.setOutputType(DiagnosticsOutputType.STDOUT);
        dConfig.setLogDirectory("random/directory");
        dConfig.setFileNamePrefix("randomPrefix");
        dConfig.setAutoOffDurationInMinutes(5);
        dConfig.setIncludeEpochTime(false);

        diagnostics.setConfig(dConfig);

        assertTrueEventually(() -> {
            String log = sb.toString();
            assertContains(log, "Diagnostics configs overridden by property:");

            assertContains(log, Diagnostics.FILENAME_PREFIX.getName() + " = "
                    + properties.get(Diagnostics.FILENAME_PREFIX.getName()));

            assertContains(log, Diagnostics.INCLUDE_EPOCH_TIME.getName() + " = "
                    + properties.get(Diagnostics.INCLUDE_EPOCH_TIME.getName()));

            assertContains(log, Diagnostics.MAX_ROLLED_FILE_COUNT.getName() + " = "
                    + properties.get(Diagnostics.MAX_ROLLED_FILE_COUNT.getName()));

            assertContains(log, Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName() + " = "
                    + properties.get(Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName()));

            assertContains(log, Diagnostics.DIRECTORY.getName() + " = "
                    + properties.get(Diagnostics.DIRECTORY.getName()));

            assertContains(log, Diagnostics.OUTPUT_TYPE.getName() + " = "
                    + properties.get(Diagnostics.OUTPUT_TYPE.getName()));
        });
    }

    @Test
    public void testStaticEnabled_cannotDisabledDynamically() {
        Assume.assumeTrue(configSource == ConfigSources.EnabledStatically);
        // If service is enabled or disabled explicitly over system properties a.k.a. static config,
        // then dynamic config cannot be applied.

        assertThrows(IllegalStateException.class,
                () -> diagnostics.setConfig(new DiagnosticsConfig().setEnabled(false)));
    }

    @Test
    public void testStaticDisabled_cannotEnabledDynamically() {
        Assume.assumeTrue(configSource == ConfigSources.DisabledStatically);
        // If service is enabled or disabled explicitly over system properties a.k.a. static config,
        // then dynamic config cannot be applied.

        assertThrows(IllegalStateException.class,
                () -> diagnostics.setConfig(new DiagnosticsConfig().setEnabled(true)));
    }

    private void disablePlugins() {
        properties.put(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "0");
        properties.put(EventQueuePlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(InvocationProfilerPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(MemberHeartbeatPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(MetricsPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(OperationHeartbeatPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(PendingInvocationsPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(SlowOperationPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "0");
        properties.put(SystemLogPlugin.ENABLED.getName(), "false");
        properties.put(SystemLogPlugin.LOG_PARTITIONS.getName(), "false");

        Properties sourceProps = new Properties();
        sourceProps.putAll(properties);
        expectedHzProperties = new HazelcastProperties(sourceProps);

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

    void assertInvocationSamplePlugin(boolean status) {
        InvocationSamplePlugin plugin = getPlugin(InvocationSamplePlugin.class);

        // expected properties
        HazelcastProperty samplePeriod = new HazelcastProperty(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty slowThreshold = new HazelcastProperty(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName())), TimeUnit.SECONDS);

        HazelcastProperty maxCount = new HazelcastProperty(InvocationSamplePlugin.SLOW_MAX_COUNT.getName(),
                properties.get(InvocationSamplePlugin.SLOW_MAX_COUNT.getName()));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod),
                plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(slowThreshold),
                plugin.getThresholdMillis());
        assertEquals(expectedHzProperties.getInteger(maxCount),
                plugin.getMaxCount());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
        }
    }

    void assertEventQueuePlugin(boolean status) {
        EventQueuePlugin plugin = getPlugin(EventQueuePlugin.class);

        // expected properties
        HazelcastProperty period = new HazelcastProperty(EventQueuePlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(EventQueuePlugin.PERIOD_SECONDS.getName())), TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(EventQueuePlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(EventQueuePlugin.THRESHOLD.getName())));

        HazelcastProperty samples = new HazelcastProperty(EventQueuePlugin.SAMPLES.getName(),
                properties.get(EventQueuePlugin.SAMPLES.getName()));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period),
                plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold),
                plugin.getThreshold());
        assertEquals(expectedHzProperties.getInteger(samples),
                plugin.getSamples());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(EventQueuePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(EventQueuePlugin.class));
        }
    }

    void assertBuildInfoPlugin(boolean status) {
        BuildInfoPlugin plugin = getPlugin(BuildInfoPlugin.class);
        assertEquals(status, plugin.isActive());
        assertEquals(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS, plugin.getPeriodMillis());
    }

    void assertConfigPropertiesPlugin(boolean status) {
        ConfigPropertiesPlugin plugin = getPlugin(ConfigPropertiesPlugin.class);
        assertEquals(status, plugin.isActive());
        assertEquals(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS, plugin.getPeriodMillis());
    }

    void assertInvocationProfilerPlugin(boolean status) {
        InvocationProfilerPlugin plugin = getPlugin(InvocationProfilerPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(InvocationProfilerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(InvocationProfilerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);


        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(InvocationProfilerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(InvocationProfilerPlugin.class));
        }
    }

    void assertMemberHazelcastInstanceInfoPlugin(boolean status) {
        MemberHazelcastInstanceInfoPlugin plugin = getPlugin(MemberHazelcastInstanceInfoPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MemberHazelcastInstanceInfoPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(MemberHazelcastInstanceInfoPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MemberHazelcastInstanceInfoPlugin.class));
        }
    }

    void assertMemberHeartbeatPlugin(boolean status) {
        MemberHeartbeatPlugin plugin = getPlugin(MemberHeartbeatPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MemberHeartbeatPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MemberHeartbeatPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty maxDeviation = new HazelcastProperty(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(),
                Integer.valueOf(properties.get(MemberHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(maxDeviation), plugin.getMaxDeviationPercentage());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(MemberHeartbeatPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MemberHeartbeatPlugin.class));
        }
    }

    void assertOperationHeartbeatPlugin(boolean status) {
        OperationHeartbeatPlugin plugin = getPlugin(OperationHeartbeatPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(OperationHeartbeatPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationHeartbeatPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty maxDeviation = new HazelcastProperty(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName(),
                Integer.valueOf(properties.get(OperationHeartbeatPlugin.MAX_DEVIATION_PERCENTAGE.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(maxDeviation), plugin.getMaxDeviationPercentage());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(OperationHeartbeatPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationHeartbeatPlugin.class));
        }
    }

    void assertMetricsPlugin(boolean status) {
        MetricsPlugin plugin = getPlugin(MetricsPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(MetricsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(MetricsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(MetricsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(MetricsPlugin.class));
        }
    }

    void assertNetworkingImbalancePlugin(boolean status) {
        NetworkingImbalancePlugin plugin = getPlugin(NetworkingImbalancePlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(NetworkingImbalancePlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        Server server = TestUtil.getNode(hz).getServer();

        if ((server instanceof TcpServer tcpServer) && (tcpServer.getNetworking() instanceof NioNetworking)) {
            assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());
            assertEquals(status, plugin.isActive());
        } else {
            // networking is null
            assertEquals(0, plugin.getPeriodMillis());
            assertFalse(plugin.isActive());
        }

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(NetworkingImbalancePlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(NetworkingImbalancePlugin.class));
        }
    }

    void assertOperationProfilerPlugin(boolean status) {
        OperationProfilerPlugin plugin = getPlugin(OperationProfilerPlugin.class);

        HazelcastProperty samplePeriod = new HazelcastProperty(OperationProfilerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationProfilerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(samplePeriod), plugin.getPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(OperationProfilerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationProfilerPlugin.class));
        }
    }

    void assertOperationThreadSamplerPlugin(boolean status) {
        OperationThreadSamplerPlugin plugin = getPlugin(OperationThreadSamplerPlugin.class);

        HazelcastProperty period = new HazelcastProperty(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OperationThreadSamplerPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty samplerPeriod = new HazelcastProperty(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName(),
                Integer.valueOf(properties.get(OperationThreadSamplerPlugin.SAMPLER_PERIOD_MILLIS.getName())),
                TimeUnit.MILLISECONDS);

        HazelcastProperty includeName = new HazelcastProperty(OperationThreadSamplerPlugin.INCLUDE_NAME.getName(),
                Boolean.parseBoolean(properties.get(OperationThreadSamplerPlugin.INCLUDE_NAME.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getMillis(samplerPeriod), plugin.getSamplerPeriodMillis());
        assertEquals(expectedHzProperties.getBoolean(includeName), plugin.getIncludeName());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(OperationThreadSamplerPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OperationThreadSamplerPlugin.class));
        }
    }

    void assertOverloadedConnectionsPlugin(boolean status) {
        OverloadedConnectionsPlugin plugin = getPlugin(OverloadedConnectionsPlugin.class);

        HazelcastProperty period = new HazelcastProperty(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(OverloadedConnectionsPlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.THRESHOLD.getName())));

        HazelcastProperty samples = new HazelcastProperty(OverloadedConnectionsPlugin.SAMPLES.getName(),
                Integer.valueOf(properties.get(OverloadedConnectionsPlugin.SAMPLES.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold), plugin.getThreshold());
        assertEquals(expectedHzProperties.getInteger(samples), plugin.getSamples());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(OverloadedConnectionsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(OverloadedConnectionsPlugin.class));
        }
    }

    void assertPendingInvocationsPlugin(boolean status) {
        PendingInvocationsPlugin plugin = getPlugin(PendingInvocationsPlugin.class);

        HazelcastProperty period = new HazelcastProperty(PendingInvocationsPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(PendingInvocationsPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        HazelcastProperty threshold = new HazelcastProperty(PendingInvocationsPlugin.THRESHOLD.getName(),
                Integer.valueOf(properties.get(PendingInvocationsPlugin.THRESHOLD.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());
        assertEquals(expectedHzProperties.getInteger(threshold), plugin.getThreshold());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(PendingInvocationsPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(PendingInvocationsPlugin.class));
        }
    }

    void assertSlowOperationPlugin(boolean status) {
        SlowOperationPlugin plugin = getPlugin(SlowOperationPlugin.class);

        HazelcastProperty period = new HazelcastProperty(SlowOperationPlugin.PERIOD_SECONDS.getName(),
                Integer.valueOf(properties.get(SlowOperationPlugin.PERIOD_SECONDS.getName())),
                TimeUnit.SECONDS);

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getMillis(period), plugin.getPeriodMillis());

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(SlowOperationPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(SlowOperationPlugin.class));
        }
    }

    void assertStoreLatencyPlugin() {
        StoreLatencyPlugin plugin = getPlugin(StoreLatencyPlugin.class);

        // Since these tests are targeted dynamic updates, StoreLatencyPlugin will be null always.
        // Because it's a non-dynamic manageable plugin. See DiagnosticsNonDynamicPluginTests.
        assertNull(plugin);
    }

    void assertSystemLogPlugin(boolean status) {
        SystemLogPlugin plugin = getPlugin(SystemLogPlugin.class);

        HazelcastProperty enabled = new HazelcastProperty(SystemLogPlugin.ENABLED.getName(),
                Boolean.parseBoolean(properties.get(SystemLogPlugin.ENABLED.getName())));

        HazelcastProperty logPartitions = new HazelcastProperty(SystemLogPlugin.LOG_PARTITIONS.getName(),
                Boolean.parseBoolean(properties.get(SystemLogPlugin.LOG_PARTITIONS.getName())));

        assertEquals(status, plugin.isActive());
        assertEquals(expectedHzProperties.getBoolean(logPartitions), plugin.getLogPartitions());
        assertEquals(expectedHzProperties.getBoolean(enabled), plugin.getPeriodMillis() == 1000);

        if (plugin.isActive()) {
            assertNotNull(diagnostics.getFutureOf(SystemLogPlugin.class));
        } else {
            assertNull(diagnostics.getFutureOf(SystemLogPlugin.class));
        }
    }

    void assertSystemPropertiesPlugin(boolean status) {
        SystemPropertiesPlugin plugin = getPlugin(SystemPropertiesPlugin.class);
        assertEquals(status, plugin.isActive());
        assertEquals(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS, plugin.getPeriodMillis());
    }

    private <P extends DiagnosticsPlugin> P getPlugin(Class<P> clazz) {
        // have to use reflection since the service will return null when it's not enabled.
        Method getPluginInstanceMethod;
        try {
            getPluginInstanceMethod = Diagnostics.class.getDeclaredMethod("getPluginInstance", Class.class);
            getPluginInstanceMethod.setAccessible(true);
            return (P) getPluginInstanceMethod.invoke(diagnostics, clazz);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
