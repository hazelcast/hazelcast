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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.logging.Logger.getLogger;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsTest extends HazelcastTestSupport {

    @Test
    public void testDisabledByDefault() {
        Config config = new Config();
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);

        assertFalse("Diagnostics should be disabled by default", hazelcastProperties.getBoolean(Diagnostics.ENABLED));
    }

    @Test
    public void whenFileNamePrefixSet() {
        Config config = new Config().setProperty(Diagnostics.FILENAME_PREFIX.getName(), "foobar");
        HazelcastProperties hzProperties = new HazelcastProperties(config);

        Diagnostics diagnostics = new Diagnostics("diagnostics", mockLoggingService(), "hz", hzProperties);
        assertStartsWith("foobar-diagnostics", diagnostics.getFileName());
    }

    @Test
    public void whenFileNamePrefixNotSet() {
        Config config = new Config();
        HazelcastProperties hzProperties = new HazelcastProperties(config);

        Diagnostics diagnostics = new Diagnostics("diagnostics", mockLoggingService(), "hz", hzProperties);
        assertStartsWith("diagnostics", diagnostics.getFileName());
    }

    @Test(expected = NullPointerException.class)
    public void register_whenNullPlugin() throws Exception {
        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();
        diagnostics.register(null);
    }

    @Test
    public void register_whenMonitorDisabled() throws Exception {
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(1L);

        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "false"));
        diagnostics.start();
        diagnostics.register(plugin);

        assertEquals(0, diagnostics.staticTasks.get().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_whenMonitorEnabled_andPluginReturnsValueSmallerThanMinesOne() throws Exception {
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(-2L);

        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();
        diagnostics.register(plugin);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginDisabled() throws Exception {
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(0L);

        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();
        diagnostics.register(plugin);

        assertEquals(0, diagnostics.staticTasks.get().length);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginStatic() throws Exception {
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS);

        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();
        diagnostics.register(plugin);

        assertArrayEquals(new DiagnosticsPlugin[]{plugin}, diagnostics.staticTasks.get());
    }

    @Test
    public void start_whenDisabled() throws Exception {
        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "false"));
        diagnostics.start();

        assertNull("DiagnosticsLogFile should be null", diagnostics.diagnosticsLog);
    }

    @Test
    public void start_whenEnabled() throws Exception {
        Diagnostics diagnostics = newDiagnostics(new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();

        assertNotNull("DiagnosticsLogFile should not be null", diagnostics.diagnosticsLog);
    }

    @Test
    public void test_DiagnosticsOutputType() throws Exception {
        assertEquals(DiagnosticsOutputType.FILE, DiagnosticsOutputType.valueOf("FILE"));
        assertEquals(DiagnosticsOutputType.STDOUT, DiagnosticsOutputType.valueOf("STDOUT"));
        assertEquals(DiagnosticsOutputType.LOGGER, DiagnosticsOutputType.valueOf("LOGGER"));

        Config config = new Config();
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setOutputType(DiagnosticsOutputType.FILE);
        Diagnostics diagnostics = newDiagnostics(config);
        diagnostics.setConfig(diagnosticsConfig);
        assertEquals(DiagnosticsLogFile.class, diagnostics.newLog(diagnostics).getClass());
        diagnosticsConfig = new DiagnosticsConfig()
                .setOutputType(DiagnosticsOutputType.STDOUT);
        diagnostics = newDiagnostics(config);
        diagnostics.setConfig(diagnosticsConfig);
        assertEquals(DiagnosticsStdout.class, diagnostics.newLog(diagnostics).getClass());
        diagnosticsConfig.setOutputType(DiagnosticsOutputType.LOGGER);
        diagnostics = newDiagnostics(config);
        diagnostics.setConfig(diagnosticsConfig);
        assertEquals(DiagnosticsLogger.class, diagnostics.newLog(diagnostics).getClass());
    }

    @Test
    public void test_enabledAfterStart() throws Exception {

        Config config = new Config();
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig();
        Diagnostics diagnostics = newDiagnostics(config);

        diagnostics.start();
        registerSomePlugins(diagnostics);

        // plugins registered but not scheduled since diagnostics is not enabled
        assertPluginsWhenDiagnosticsDisabled(diagnostics);
        assertGreaterOrEquals("Static plugin registered", diagnostics.staticTasks.get().length, 0);

        // enable diagnostics at runtime
        diagnosticsConfig.setEnabled(true);
        diagnostics.setConfig(diagnosticsConfig);

        assertPluginsWhenDiagnosticsEnabled(diagnostics);
    }


    @Test
    public void test_disabledAfterStart() throws Exception {
        Config config = new Config();
        Diagnostics diagnostics = newDiagnostics(config);
        diagnostics.start();
        diagnostics.setConfig(new DiagnosticsConfig().setEnabled(true));
        registerSomePlugins(diagnostics);

        verify(diagnostics.getPluginInstance(BuildInfoPlugin.class), times(1)).onStart();
        verify(diagnostics.getPluginInstance(BuildInfoPlugin.class), times(0)).onShutdown();
        verify(diagnostics.getPluginInstance(InvocationSamplePlugin.class), times(0)).onShutdown();
        verify(diagnostics.getPluginInstance(InvocationSamplePlugin.class), times(1)).onStart();

        diagnostics.setConfig(new DiagnosticsConfig().setEnabled(false));

        verify(diagnostics.getPluginInstance(BuildInfoPlugin.class), times(1)).onStart();
        // static plugins runs once hence no future
        verify(diagnostics.getPluginInstance(BuildInfoPlugin.class), times(1)).onShutdown();
        verify(diagnostics.getPluginInstance(InvocationSamplePlugin.class), times(1)).onShutdown();
        verify(diagnostics.getPluginInstance(InvocationSamplePlugin.class), times(1)).onStart();

        assertPluginsWhenDiagnosticsDisabled(diagnostics);
    }

    private static void assertPluginsWhenDiagnosticsEnabled(Diagnostics diagnostics) {
        assertNotNull(diagnostics.getPluginInstance(BuildInfoPlugin.class));
        assertNotNull(diagnostics.getPluginInstance(InvocationSamplePlugin.class));
        // static plugins runs once hence no future
        assertGreaterOrEquals("Static plugin not registered", diagnostics.staticTasks.get().length, 1);
        assertNull(diagnostics.getFutureOf(BuildInfoPlugin.class));
        // dynamic plugin runs periodically, future is not null
        assertNotNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
    }

    private static void assertPluginsWhenDiagnosticsDisabled(Diagnostics diagnostics) {
        assertNotNull(diagnostics.getPluginInstance(BuildInfoPlugin.class));
        assertNotNull(diagnostics.getPluginInstance(InvocationSamplePlugin.class));
        assertNull(diagnostics.getFutureOf(BuildInfoPlugin.class));
        assertNull(diagnostics.getFutureOf(InvocationSamplePlugin.class));
        assertFalse(diagnostics.isEnabled());
    }

    private void registerSomePlugins(Diagnostics diagnostics) {
        DiagnosticsPlugin pluginStatic = mock(BuildInfoPlugin.class);
        when(pluginStatic.canBeEnabledDynamically()).thenReturn(true);
        when(pluginStatic.getPeriodMillis()).thenReturn(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS);
        diagnostics.register(pluginStatic);

        DiagnosticsPlugin pluginDynamic = mock(InvocationSamplePlugin.class);
        when(pluginDynamic.canBeEnabledDynamically()).thenReturn(true);
        when(pluginDynamic.getPeriodMillis()).thenReturn(100_000L);
        diagnostics.register(pluginDynamic);
    }

    @Test
    public void test_DiagnosticsFileOutput() throws Exception {
        Diagnostics diagnostics = newDiagnostics(new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "false")
                .setProperty(Diagnostics.MAX_ROLLED_FILE_COUNT.getName(), "3")
                .setProperty(Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName(), "1"));

        DiagnosticsLog log = Diagnostics.newLog(diagnostics);

        assertEquals(3, ((DiagnosticsLogFile) log).getMaxRollingFileCount());
        assertEquals(1024 * 1024, ((DiagnosticsLogFile) log).getMaxRollingFileSizeBytes());
    }

    private Diagnostics newDiagnostics(Config config) throws Exception {
        Address address = new Address("127.0.0.1", 5701);
        String addressString = address.getHost().replace(":", "_") + "#" + address.getPort();
        String name = "diagnostics-" + addressString + "-" + currentTimeMillis();

        return new Diagnostics(name, mockLoggingService(), "hz", new HazelcastProperties(config));
    }

    private LoggingService mockLoggingService() {
        LoggingService mock = mock(LoggingService.class);
        when(mock.getLogger(Diagnostics.class)).thenReturn(getLogger(Diagnostics.class));
        return mock;
    }
}
