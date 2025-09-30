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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDiagnosticsTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
        hazelcastFactory.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void testClientDiagnostics() {
        ClientConfig clientConfig = new ClientConfig();
        String fileNamePrefix = "testLogs";

        DiagnosticsConfig dConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setOutputType(DiagnosticsOutputType.FILE)
                .setLogDirectory(temporaryFolder.getRoot().getAbsolutePath())
                .setFileNamePrefix(fileNamePrefix)
                .setMaxRolledFileCount(5)
                .setMaxRolledFileSizeInMB(30)
                .setAutoOffDurationInMinutes(5)
                .setIncludeEpochTime(true);
        dConfig.getPluginProperties().putAll(getPluginProperties());

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        clientImpl.getDiagnostics().setConfig(dConfig);
        DiagnosticsConfig diagnosticsConfig = clientImpl.getDiagnostics().getDiagnosticsConfig();

        assertTrue(diagnosticsConfig.isEnabled());
        assertEquals(dConfig.getOutputType(), diagnosticsConfig.getOutputType());
        assertEquals(dConfig.getLogDirectory(), diagnosticsConfig.getLogDirectory());
        assertEquals(dConfig.getFileNamePrefix(), diagnosticsConfig.getFileNamePrefix());
        assertEquals(dConfig.getMaxRolledFileCount(), diagnosticsConfig.getMaxRolledFileCount());
        assertEquals(dConfig.getMaxRolledFileSizeInMB(), diagnosticsConfig.getMaxRolledFileSizeInMB(), 0);
        assertTrue(dConfig.isIncludeEpochTime());
        assertEquals(dConfig.getPluginProperties(), diagnosticsConfig.getPluginProperties());
        assertEquals(dConfig.getAutoOffDurationInMinutes(), diagnosticsConfig.getAutoOffDurationInMinutes());

        File[] matchingFiles = temporaryFolder.getRoot().listFiles((dir, name) -> name.startsWith(fileNamePrefix));
        assert matchingFiles != null;
        assertTrueEventually(() -> assertTrue(matchingFiles.length > 0));
    }

    private Map<String, String> getPluginProperties() {
        Map<String, String> properties = new ConcurrentHashMap<>();
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
        return properties;
    }
}
