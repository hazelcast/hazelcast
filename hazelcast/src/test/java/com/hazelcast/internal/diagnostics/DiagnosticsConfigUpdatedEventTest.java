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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.events.DiagnosticsConfigUpdatedEvent;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiagnosticsConfigUpdatedEventTest extends AbstractDiagnosticsPluginTest {

    private List<HazelcastInstance> hazelcastInstances = new ArrayList<>();
    private TestHazelcastInstanceFactory instanceFactory;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() {
        instanceFactory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = instanceFactory.newInstances(regularInstanceConfig(), 3);
        hazelcastInstances.addAll(Arrays.asList(instances));
    }

    @Test
    public void testDiagnosticsConfigUpdatedEvent() throws IOException {

        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setAutoOffDurationInMinutes(1)
                .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "100")
                .setLogDirectory(folder.newFolder().getAbsolutePath())
                .setFileNamePrefix("hz")
                .setMaxRolledFileSizeInMB(9)
                .setOutputType(DiagnosticsOutputType.STDOUT)
                .setIncludeEpochTime(true);

        // assert for enabled
        CountDownLatch latchForEnabled = new CountDownLatch(hazelcastInstances.size());
        List<Event> eventsForEnabled = new ArrayList<>();
        setListener(latchForEnabled, eventsForEnabled);
        setDiagnosticsConfig(diagnosticsConfig);
        assertEventEmittedEventually(diagnosticsConfig, latchForEnabled, eventsForEnabled);


        // assert for disabled
        CountDownLatch latchForDisabled = new CountDownLatch(hazelcastInstances.size());
        List<Event> eventsForDisabled = new ArrayList<>();
        setListener(latchForDisabled, eventsForDisabled);
        diagnosticsConfig.setEnabled(false);
        setDiagnosticsConfig(diagnosticsConfig);
        assertEventEmittedEventually(diagnosticsConfig, latchForDisabled, eventsForDisabled);
    }

    @Test
    public void testDiagnosticsConfigUpdatedEvent_whenNewMemberJoins() throws IOException {

        DiagnosticsConfig diagnosticsConfig = null;
        try {
            diagnosticsConfig = new DiagnosticsConfig()
                    .setEnabled(true)
                    .setAutoOffDurationInMinutes(1)
                    .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "100")
                    .setLogDirectory(folder.newFolder().getAbsolutePath())
                    .setFileNamePrefix("hz")
                    .setMaxRolledFileSizeInMB(9)
                    .setOutputType(DiagnosticsOutputType.FILE)
                    .setIncludeEpochTime(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // assert for enabled
        CountDownLatch latchForEnabled = new CountDownLatch(hazelcastInstances.size());
        List<Event> eventsForEnabled = new ArrayList<>();
        setListener(latchForEnabled, eventsForEnabled);
        setDiagnosticsConfig(diagnosticsConfig);

        assertEventEmittedEventually(diagnosticsConfig, latchForEnabled, eventsForEnabled);

        // Add new member and combine the list
        hazelcastInstances.add(instanceFactory.newHazelcastInstance(regularInstanceConfig()));

        assertTrueEventually(() -> assertAllInSafeState(hazelcastInstances));

        // verify all members have diagnostics enabled
        for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hazelcastInstance);
            assertTrue(hazelcastInstance.getName() + " not enabled the diagnostics.", nodeEngine.getDiagnostics().isEnabled());
        }
    }

    private void setListener(CountDownLatch latch, List<Event> events) {
        for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hazelcastInstance);
            ManagementCenterService mcService = nodeEngine.getManagementCenterService();
            mcService.setEventListener(event -> {
                events.add(event);
                latch.countDown();
            });
        }
    }

    private void assertEventEmittedEventually(DiagnosticsConfig expCfg, CountDownLatch latch, List<Event> events) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<String> uuids = new ArrayList<>(hazelcastInstances.stream()
                .map(Accessors::getNodeEngineImpl)
                .map(nodeEngine -> nodeEngine.getLocalMember().getUuid().toString())
                .toList());


        // All members should emit the event after applying the config since each have separated Diagnostics service

        for (Event event : events) {
            if (event instanceof DiagnosticsConfigUpdatedEvent dcEvent) {
                JsonObject eventCfg = dcEvent.toJson();

                assertEquals(expCfg.isEnabled(), eventCfg.getBoolean("enabled", false));
                assertEquals(expCfg.getMaxRolledFileSizeInMB(), eventCfg.getFloat("max-rolled-file-size-in-mb", 0f), 0.1f);
                assertEquals(expCfg.getMaxRolledFileCount(), eventCfg.getInt("max-rolled-file-count", 0));
                assertEquals(expCfg.isIncludeEpochTime(), eventCfg.getBoolean("include-epoch-time", false));
                assertEquals(expCfg.getLogDirectory(), eventCfg.getString("log-directory", ""));
                assertEquals(expCfg.getFileNamePrefix(), eventCfg.getString("file-name-prefix", ""));
                assertEquals(expCfg.getOutputType().name(), eventCfg.getString("output-type", ""));
                assertEquals(expCfg.getAutoOffDurationInMinutes(), eventCfg.getInt("auto-off-timer-in-minutes", 0));

                uuids.remove(eventCfg.getString("member-id", ""));

                JsonObject eventPluginProperties = eventCfg.get("plugin-properties").asObject();
                for (String key : expCfg.getPluginProperties().keySet()) {
                    assertEquals(expCfg.getPluginProperties().get(key),
                            eventPluginProperties.getString(key, ""));
                }
            }
        }

        assertTrue(uuids.isEmpty());
    }

    private void setDiagnosticsConfig(DiagnosticsConfig diagnosticsConfig) {
        ((DynamicConfigurationAwareConfig) hazelcastInstances.get(0).getConfig()).setDiagnosticsConfig(diagnosticsConfig);
    }
}
