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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsAutoOffTests extends AbstractDiagnosticsPluginTest {

    private HazelcastInstance hz;
    Diagnostics diagnostics;
    TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    public void setup(Config config) {
        hz = factory.newHazelcastInstance(config);
        diagnostics = TestUtil.getNode(hz).getNodeEngine().getDiagnostics();
        // shorten the auto-off timer for the test, won't work when statically enabled.
        diagnostics.autoOffDurationUnit = TimeUnit.SECONDS;
    }

    @Test
    public void testDiagnosticsTurnedOffAutomatically_whenEnabledDynamically() {
        setup(new Config());
        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(3));

        assertTrue(diagnostics.isEnabled());
        assertDiagnosticsTurnedOffAutomatically();
    }

    @Test
    public void testDiagnosticsTurnedOffAutomatically_whenEnabledDynamicallyManyTimes() {
        setup(new Config());

        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(1));
        assertTrue(diagnostics.isEnabled());
        assertDiagnosticsTurnedOffAutomatically();

        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(1));
        assertTrue(diagnostics.isEnabled());
        assertDiagnosticsTurnedOffAutomatically();

        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(1));
        assertTrue(diagnostics.isEnabled());
        assertDiagnosticsTurnedOffAutomatically();
    }

    @Test
    public void testDiagnosticsStays_autoOffDisabledByDefault_overStaticConfig() {
        Config config = new Config();
        config.setProperty(Diagnostics.ENABLED.getName(), "true");
        setup(config);

        assertAutoOffNotSet(diagnostics);
        assertTrue(diagnostics.isEnabled());
    }

    @Test
    public void testDiagnosticsStays_autoOffDisabledByDefault_overDynamicConfig() {
        setup(new Config());
        setDynamicConfig(new DiagnosticsConfig().setEnabled(true));

        assertAutoOffNotSet(diagnostics);
        assertTrue(diagnostics.isEnabled());
    }

    @Test
    public void testDiagnosticsStays_whenAutoOffDisabledDynamically() {
        setup(new Config());

        setDynamicConfig(new DiagnosticsConfig()
                .setEnabled(true).setAutoOffDurationInMinutes(-1));

        assertAutoOffNotSet(diagnostics);
        assertTrue(diagnostics.isEnabled());
    }

    @Test
    public void testAutoOff_doesNotFail() {
        setup(new Config());
        // Capture logs
        StringBuilder sbLogs = captureLogs();

        setDynamicConfig(new DiagnosticsConfig().setEnabled(false).setAutoOffDurationInMinutes(3));

        var mockDiagnostics = Mockito.spy(diagnostics);

        // Arrange that the setConfig method throws an exception
        assertAutoOffNotSet(mockDiagnostics);
        mockDiagnostics.setConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(5));
        doThrow(new RuntimeException("Test_SetConfig_Failed")).when(mockDiagnostics).setConfig(Mockito.any());

        assertTrueEventually(() -> {
            assertTrue(mockDiagnostics.isEnabled());
            assertFalse(mockDiagnostics.getAutoOffFuture().isDone());
            assertContains(sbLogs.toString(), "Test_SetConfig_Failed");
            assertContains(sbLogs.toString(), "Auto off failed to disable diagnostics.");
        }, 15);

        // When everything is ok, the auto off should work.
        Mockito.reset(mockDiagnostics);
        assertDiagnosticsTurnedOffAutomatically(mockDiagnostics);
    }

    @Test
    public void testAutoOff_cancelledWhenDiagnosticsDisabled() {
        setup(new Config());
        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(1));

        // Capture logs
        StringBuilder sbLogs = captureLogs();

        setDynamicConfig(new DiagnosticsConfig()
                .setEnabled(false));
        assertFalse(diagnostics.isEnabled());
        assertAutoOffNotSet(diagnostics);

        assertTrueEventually(() -> assertContains(sbLogs.toString(), "Existing auto off future cancelled."));
    }

    @Test
    public void testAutoOffSkipped_whenNonDynamicPluginExist() {
        // Non-dynamic plugins prevent Diagnostics to be disabled at runtime. They required the node to be restarted.
        // So, auto off should be skipped because Diagnostics cannot be disabled.

        Config config = new Config();
        // Enable a non-dynamic plugin
        config.setProperty(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "30");
        // Enable the diagnostics
        config.setProperty(Diagnostics.ENABLED.getName(), "true");
        setup(config);

        assertTrueEventually(() -> {
            assertTrue(diagnostics.isEnabled());
            assertAutoOffNotSet(diagnostics);
        });

        // non-dynamic plugins can be set only over static config. So that we cannot set config dynamically since
        // first config is done over static config.
        assertThrows(IllegalStateException.class, () -> {
            diagnostics.setConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(1));
        });
    }

    @Test
    public void testAfterAutoOff_newJoinerReceivesDisabledConfig() {
        // After enabling diagnostics config along with auto-off, and diagnostics turned off automatically, if a
        // new member joins, it should receive the config as disabled.
        setup(new Config());

        // Enable diagnostics with auto-off for 5 secs
        setDynamicConfig(new DiagnosticsConfig().setEnabled(true).setAutoOffDurationInMinutes(5));
        assertTrue(diagnostics.isEnabled());

        // Auto off should disable diagnostics after 5 secs
        assertTrueEventually(() -> {
            assertFalse(diagnostics.isEnabled());
            assertTrue(diagnostics.getAutoOffFuture() == null || diagnostics.getAutoOffFuture().isDone());
        });

        // Create a new member to join the cluster
        HazelcastInstance newMember = factory.newHazelcastInstance(new Config());
        assertAllInSafeState(factory.getAllHazelcastInstances());

        // The new member should receive the diagnostics config as disabled
        Diagnostics newMemberDiagnostics = TestUtil.getNode(newMember).getNodeEngine().getDiagnostics();
        assertFalse(newMemberDiagnostics.isEnabled());
    }

    private @Nonnull StringBuilder captureLogs() {
        StringBuilder sbLogs = new StringBuilder();
        hz.getLoggingService().addLogListener(Level.ALL, logEvent -> {
            sbLogs.append(logEvent.getLogRecord().getMessage());
            if (logEvent.getLogRecord().getThrown() != null) {
                sbLogs.append(logEvent.getLogRecord().getThrown().getMessage());
            }
        });
        return sbLogs;
    }

    private void assertAutoOffNotSet(Diagnostics diagnostics) {
        assertTrue(diagnostics.getAutoOffFuture() == null
                || diagnostics.getAutoOffFuture().isDone()
                || diagnostics.getAutoOffFuture().isCancelled());
    }

    private void assertDiagnosticsTurnedOffAutomatically() {
        assertDiagnosticsTurnedOffAutomatically(diagnostics);
    }

    private void assertDiagnosticsTurnedOffAutomatically(Diagnostics ds) {
        // either future is null means already disabled, or not done yet.
        assertTrue(ds.getAutoOffFuture() == null || !ds.getAutoOffFuture().isDone());
        assertTrueEventually(() -> {
            assertFalse(ds.isEnabled());
            assertTrue(ds.getAutoOffFuture() == null || ds.getAutoOffFuture().isDone());
            assertGreaterOrEquals("AutoOff Metric", ds.getMetricCollector().getAutoOffDisabledCount().get(), 1);
        });
    }

    private void setDynamicConfig(DiagnosticsConfig config) {
        diagnostics.setConfig(config);
    }
}
