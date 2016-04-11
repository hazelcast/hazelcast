package com.hazelcast.internal.monitors;

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
public class PerformanceMonitorTest extends HazelcastTestSupport {

    private PerformanceMonitor newPerformanceMonitor(Config config) {
        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);

        Member localMember = nodeEngineImpl.getLocalMember();
        Address address = localMember.getAddress();
        String addressString = address.getHost().replace(":", "_") + "#" + address.getPort();
        String name = "performance-" + addressString + "-" + currentTimeMillis();

        return new PerformanceMonitor(
                name,
                Logger.getLogger(PerformanceMonitor.class),
                nodeEngineImpl.getNode().getHazelcastThreadGroup(),
                nodeEngineImpl.getNode().getProperties());
    }

    @Test(expected = NullPointerException.class)
    public void register_whenNullPlugin() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "true"));
        performanceMonitor.start();
        performanceMonitor.register(null);
    }

    @Test
    public void register_whenMonitorDisabled() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "false"));

        performanceMonitor.start();
        PerformanceMonitorPlugin plugin = mock(PerformanceMonitorPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(1l);

        performanceMonitor.register(plugin);

        assertEquals(0, performanceMonitor.staticTasks.get().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_whenMonitorEnabled_andPluginReturnsValueSmallerThanMinesOne() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "true"));

        performanceMonitor.start();
        PerformanceMonitorPlugin plugin = mock(PerformanceMonitorPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(-2l);

        performanceMonitor.register(plugin);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginDisabled() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "true"));

        performanceMonitor.start();
        PerformanceMonitorPlugin plugin = mock(PerformanceMonitorPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(0l);

        performanceMonitor.register(plugin);

        assertEquals(0, performanceMonitor.staticTasks.get().length);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginStatic() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "true"));
        performanceMonitor.start();


        performanceMonitor.start();
        PerformanceMonitorPlugin plugin = mock(PerformanceMonitorPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(PerformanceMonitorPlugin.STATIC);

        performanceMonitor.register(plugin);

        assertArrayEquals(new PerformanceMonitorPlugin[]{plugin}, performanceMonitor.staticTasks.get());
    }

    @Test
    public void start_whenDisabled() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "false"));
        performanceMonitor.start();

        assertNull(performanceMonitor.performanceLog);
    }

    @Test
    public void start_whenEnabled() {
        PerformanceMonitor performanceMonitor = newPerformanceMonitor(
                new Config().setProperty(PerformanceMonitor.ENABLED.getName(), "true"));
        performanceMonitor.start();

        assertNotNull(performanceMonitor.performanceLog);
    }
}
