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
import com.hazelcast.client.impl.protocol.codec.MCGetDiagnosticsConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ConfigMessageTaskTest;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.internal.diagnostics.InvocationSamplePlugin;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetDiagnosticsConfigMessageTaskTest extends ConfigMessageTaskTest<GetDiagnosticsConfigMessageTask> {


    private DiagnosticsConfig sampleConfig = new DiagnosticsConfig()
            .setEnabled(true)
            .setOutputType(DiagnosticsOutputType.STDOUT)
            .setIncludeEpochTime(true)
            .setMaxRolledFileSizeInMB(10)
            .setMaxRolledFileCount(5)
            .setLogDirectory("logs")
            .setFileNamePrefix("diagnostics")
            .setProperty(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "42")
            .setAutoOffDurationInMinutes(60);


    @Test
    public void testGetDiagnosticsConfig() {
        ClientMessage clientMessage = getSampleClientMessage(sampleConfig);
        GetDiagnosticsConfigMessageTask task = createMessageTask(clientMessage);
        ClientMessage response = task.encodeResponse(task.call());

        assertMessagesEqual(
                getSampleClientMessage(sampleConfig),
                response
        );
    }

    private void assertMessagesEqual(ClientMessage sampleClientMessage, ClientMessage response) {

        assertEquals(sampleClientMessage.getMessageType(), response.getMessageType());

        MCGetDiagnosticsConfigCodec.ResponseParameters responseParams =
                MCGetDiagnosticsConfigCodec.decodeResponse(response);

        MCGetDiagnosticsConfigCodec.ResponseParameters sampleParams =
                MCGetDiagnosticsConfigCodec.decodeResponse(sampleClientMessage);

        assertEquals(sampleParams.enabled, responseParams.enabled);
        assertEquals(sampleParams.outputType, responseParams.outputType);
        assertEquals(sampleParams.includeEpochTime, responseParams.includeEpochTime);
        assertEquals(sampleParams.maxRolledFileSizeInMB, responseParams.maxRolledFileSizeInMB, 0);
        assertEquals(sampleParams.maxRolledFileCount, responseParams.maxRolledFileCount);
        assertEquals(sampleParams.logDirectory, responseParams.logDirectory);
        assertEquals(sampleParams.fileNamePrefix, responseParams.fileNamePrefix);
        assertEquals(sampleParams.properties, responseParams.properties);
        assertEquals(sampleParams.autoOffDurationInMinutes, responseParams.autoOffDurationInMinutes);
        assertTrue(responseParams.canBeConfiguredDynamically);
    }

    @Override
    protected GetDiagnosticsConfigMessageTask createMessageTask(ClientMessage clientMessage) {
        NodeEngineImpl nodeImpl = mock(NodeEngineImpl.class);
        Diagnostics mockDiagnostics = mock(Diagnostics.class);
        when(nodeImpl.getDiagnostics()).thenReturn(mockDiagnostics);
        when(nodeImpl.getDiagnostics().getDiagnosticsConfig()).thenReturn(sampleConfig);
        when(nodeImpl.getDiagnostics().isConfiguredStatically()).thenReturn(false);

        return new GetDiagnosticsConfigMessageTask(clientMessage, logger, nodeImpl, mock(), mockClientEngine, mockConnection,
                mockNodeExtension, mock(), config, mock());
    }

    protected ClientMessage getSampleClientMessage(DiagnosticsConfig diagnosticsConfig) {
        return MCGetDiagnosticsConfigCodec.encodeResponse(
                diagnosticsConfig.isEnabled(),
                diagnosticsConfig.getOutputType().name(),
                diagnosticsConfig.isIncludeEpochTime(),
                diagnosticsConfig.getMaxRolledFileSizeInMB(),
                diagnosticsConfig.getMaxRolledFileCount(),
                diagnosticsConfig.getLogDirectory(),
                diagnosticsConfig.getFileNamePrefix(),
                diagnosticsConfig.getPluginProperties(),
                diagnosticsConfig.getAutoOffDurationInMinutes(),
                true
        );
    }
}
