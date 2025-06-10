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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCSetDiagnosticsConfigCodec;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ConfigMessageTaskTest;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.internal.diagnostics.OperationProfilerPlugin;
import com.hazelcast.internal.server.ServerConnection;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.security.AccessControlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetDiagnosticsConfigMessageTaskTest extends ConfigMessageTaskTest<SetDiagnosticsConfigMessageTask> {

    @Override
    protected SetDiagnosticsConfigMessageTask createMessageTask(ClientMessage clientMessage) {
        return new SetDiagnosticsConfigMessageTask(clientMessage, logger, mockNodeEngine, mock(), mockClientEngine, mockConnection,
                mockNodeExtension, mock(), config, mock());
    }

    protected DiagnosticsConfig getSampleConfig() {

        Map<String, String> properties = new HashMap<>();
        properties.put(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "9999");

        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig();
        diagnosticsConfig.setEnabled(true);
        diagnosticsConfig.setOutputType(DiagnosticsOutputType.STDOUT);
        diagnosticsConfig.setLogDirectory("usr/directory");
        diagnosticsConfig.setFileNamePrefix("my-prefix");
        diagnosticsConfig.setMaxRolledFileCount(100);
        diagnosticsConfig.setMaxRolledFileSizeInMB(10);
        diagnosticsConfig.setIncludeEpochTime(true);
        diagnosticsConfig.setAutoOffDurationInMinutes(5);
        diagnosticsConfig.getPluginProperties().putAll(properties);

        return diagnosticsConfig;
    }

    protected ClientMessage getSampleClientMessage(DiagnosticsConfig diagnosticsConfig) {
        return MCSetDiagnosticsConfigCodec.encodeRequest(
                diagnosticsConfig.isEnabled(),
                diagnosticsConfig.getOutputType().name(),
                diagnosticsConfig.isIncludeEpochTime(),
                diagnosticsConfig.getMaxRolledFileSizeInMB(),
                diagnosticsConfig.getMaxRolledFileCount(),
                diagnosticsConfig.getLogDirectory(),
                diagnosticsConfig.getFileNamePrefix(),
                diagnosticsConfig.getPluginProperties(),
                diagnosticsConfig.getAutoOffDurationInMinutes()
        );
    }

    @After
    public void tearDown() {
        // Reset to default the mockClientEngine to avoid side effects on other tests
        reset(mockClientEngine);
    }

    @Test
    public void testSetDiagnosticsConfig_Succeeds() {

        DiagnosticsConfig diagnosticsConfig = getSampleConfig();
        SetDiagnosticsConfigMessageTask task = createMessageTask(getSampleClientMessage(diagnosticsConfig));

        task.run();

        DiagnosticsConfig taskConfig = (DiagnosticsConfig) task.getConfig();

        assertEquals(diagnosticsConfig.isEnabled(), taskConfig.isEnabled());
        assertEquals(diagnosticsConfig.getOutputType(), taskConfig.getOutputType());
        assertEquals(diagnosticsConfig.getLogDirectory(), taskConfig.getLogDirectory());
        assertEquals(diagnosticsConfig.getFileNamePrefix(), taskConfig.getFileNamePrefix());
        assertEquals(diagnosticsConfig.getMaxRolledFileCount(), taskConfig.getMaxRolledFileCount());
        assertEquals(diagnosticsConfig.getMaxRolledFileSizeInMB(), taskConfig.getMaxRolledFileSizeInMB(), 0);
        assertEquals(diagnosticsConfig.isIncludeEpochTime(), taskConfig.isIncludeEpochTime());
        assertEquals(diagnosticsConfig.getAutoOffDurationInMinutes(), taskConfig.getAutoOffDurationInMinutes());
        assertEquals(diagnosticsConfig.getPluginProperties().size(), taskConfig.getPluginProperties().size());
        assertEquals(diagnosticsConfig.getPluginProperties().get(OperationProfilerPlugin.PERIOD_SECONDS.getName()),
                taskConfig.getPluginProperties().get(OperationProfilerPlugin.PERIOD_SECONDS.getName()));
    }

    @Test
    public void testSetDiagnosticsConfig_Succeeds_WhenPropsNull() {

        DiagnosticsConfig diagnosticsConfig = getSampleConfig();
        diagnosticsConfig.getPluginProperties().clear();
        SetDiagnosticsConfigMessageTask task = createMessageTask(getSampleClientMessage(diagnosticsConfig));

        task.run();

        DiagnosticsConfig taskConfig = (DiagnosticsConfig) task.getConfig();

        assertEquals(diagnosticsConfig.isEnabled(), taskConfig.isEnabled());
        assertEquals(diagnosticsConfig.getOutputType(), taskConfig.getOutputType());
        assertEquals(diagnosticsConfig.getLogDirectory(), taskConfig.getLogDirectory());
        assertEquals(diagnosticsConfig.getFileNamePrefix(), taskConfig.getFileNamePrefix());
        assertEquals(diagnosticsConfig.getMaxRolledFileCount(), taskConfig.getMaxRolledFileCount());
        assertEquals(diagnosticsConfig.getMaxRolledFileSizeInMB(), taskConfig.getMaxRolledFileSizeInMB(), 0);
        assertEquals(diagnosticsConfig.isIncludeEpochTime(), taskConfig.isIncludeEpochTime());
        assertEquals(diagnosticsConfig.getPluginProperties().size(), taskConfig.getPluginProperties().size());
        assertEquals(diagnosticsConfig.getAutoOffDurationInMinutes(), taskConfig.getAutoOffDurationInMinutes());
        assertEquals(diagnosticsConfig.getPluginProperties().get(OperationProfilerPlugin.PERIOD_SECONDS.getName()),
                taskConfig.getPluginProperties().get(OperationProfilerPlugin.PERIOD_SECONDS.getName()));
    }

    @Test
    public void testSetDiagnosticsConfig_FailsOnClientCall() {
        when(mockClientEngine.getManagementTasksChecker().isTrusted(any())).thenReturn(false);
        ArgumentCaptor<ClientMessage> argumentCaptor = ArgumentCaptor.forClass(ClientMessage.class);

        DiagnosticsConfig diagnosticsConfig = getSampleConfig();
        SetDiagnosticsConfigMessageTask task = createMessageTask(getSampleClientMessage(diagnosticsConfig));
        task.run();

        verify((ServerConnection) mockConnection).write(argumentCaptor.capture());
        List<ErrorHolder> errorHolders = ErrorsCodec.decode(argumentCaptor.getValue());
        assertEquals(errorHolders.get(0).getClassName(), AccessControlException.class.getName());
    }
}

