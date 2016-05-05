package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsTest extends HazelcastTestSupport {

    private Diagnostics newDiagnostics(Config config) {
        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);

        Member localMember = nodeEngineImpl.getLocalMember();
        Address address = localMember.getAddress();
        String addressString = address.getHost().replace(":", "_") + "#" + address.getPort();
        String name = "diagnostics-" + addressString + "-" + currentTimeMillis();

        return new Diagnostics(
                name,
                Logger.getLogger(Diagnostics.class),
                nodeEngineImpl.getNode().getHazelcastThreadGroup(),
                nodeEngineImpl.getNode().getProperties());
    }

    @Test(expected = NullPointerException.class)
    public void register_whenNullPlugin() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();
        diagnostics.register(null);
    }

    @Test
    public void register_whenMonitorDisabled() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "false"));

        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(1l);

        diagnostics.register(plugin);

        assertEquals(0, diagnostics.staticTasks.get().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_whenMonitorEnabled_andPluginReturnsValueSmallerThanMinesOne() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));

        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(-2l);

        diagnostics.register(plugin);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginDisabled() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));

        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(0l);

        diagnostics.register(plugin);

        assertEquals(0, diagnostics.staticTasks.get().length);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginStatic() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();


        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(DiagnosticsPlugin.STATIC);

        diagnostics.register(plugin);

        assertArrayEquals(new DiagnosticsPlugin[]{plugin}, diagnostics.staticTasks.get());
    }

    @Test
    public void start_whenDisabled() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "false"));
        diagnostics.start();

        assertNull(diagnostics.diagnosticsLogFile);
    }

    @Test
    public void start_whenEnabled() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));
        diagnostics.start();

        assertNotNull(diagnostics.diagnosticsLogFile);
    }
}
