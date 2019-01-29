/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Member;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

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
                "hz",
                nodeEngineImpl.getNode().getProperties());
    }

    @Test
    public void whenFileNamePrefixSet() {
        Properties properties = new Properties();
        properties.put(Diagnostics.FILENAME_PREFIX.getName(), "foobar");
        HazelcastProperties hzProperties = new HazelcastProperties(properties);

        Diagnostics diagnostics = new Diagnostics("diagnostics", Logger.getLogger(Diagnostics.class), "hz", hzProperties);
        assertEquals("foobar-diagnostics", diagnostics.baseFileName);
    }

    @Test
    public void whenFileNamePrefixNotSet() {
        Properties properties = new Properties();
        HazelcastProperties hzProperties = new HazelcastProperties(properties);

        Diagnostics diagnostics = new Diagnostics("diagnostics", Logger.getLogger(Diagnostics.class), "hz", hzProperties);
        assertEquals("diagnostics", diagnostics.baseFileName);
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
        when(plugin.getPeriodMillis()).thenReturn(1L);

        diagnostics.register(plugin);

        assertEquals(0, diagnostics.staticTasks.get().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_whenMonitorEnabled_andPluginReturnsValueSmallerThanMinesOne() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));

        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(-2L);

        diagnostics.register(plugin);
    }

    @Test
    public void register_whenMonitorEnabled_andPluginDisabled() {
        Diagnostics diagnostics = newDiagnostics(
                new Config().setProperty(Diagnostics.ENABLED.getName(), "true"));

        diagnostics.start();
        DiagnosticsPlugin plugin = mock(DiagnosticsPlugin.class);
        when(plugin.getPeriodMillis()).thenReturn(0L);

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
